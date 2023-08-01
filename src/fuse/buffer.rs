use std::{
    collections::HashMap,
    fmt::Debug,
    hash::Hash,
    num::NonZeroUsize,
    ops::Deref,
    sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use libc::c_int;
use lru::LruCache;

use super::sync::SyncFsConfig;

const BUFFER_POOL_SIZE: usize = 1024;

pub trait Buffer: Default {
    type Key: Hash + Eq + Clone + Debug;
    type Value: Default;

    fn empty(config: &SyncFsConfig, key: &Self::Key) -> Self;
    fn from_fs(config: &SyncFsConfig, key: &Self::Key) -> Result<Self, c_int>;

    fn dirty(&mut self);
    fn fsync(&mut self, config: &SyncFsConfig);
    fn key(&self) -> Self::Key;
    fn value(&self) -> &Self::Value;
}

pub trait Pool {
    type Unit;

    fn pin(&self, unit: &Self::Unit);
    fn unpin(&self, unit: &Self::Unit);
}

/// The unit of the buffer.
///
/// Lock `refcnt` first if data should be replaced.
pub struct BufferUnit<T: Buffer> {
    pub(super) bid: usize,
    pub(super) refcnt: Mutex<usize>,
    data: RwLock<T>,
}

pub struct BufferHandle<'a, P: Pool> {
    // unit: &'a BufferUnit<T>,
    // pool: &'a BufferPool<T>,
    unit: &'a P::Unit,
    pool: &'a P,
}

pub struct Alteration<K>
where
    K: Hash + Eq,
{
    free: Vec<usize>,
    lru: LruCache<K, usize>,
}

/// Lock tree: map -> alter, map -> pool, map -> free, alter -> pool
pub struct BufferPool<T: Buffer> {
    /// The pool that used to hold wrappers.
    pool: Box<[BufferUnit<T>; BUFFER_POOL_SIZE]>,

    /// The LRU replacer that decide which victim to evict.
    alter: Mutex<Alteration<T::Key>>,

    /// Map key to index in pool.
    map: RwLock<HashMap<T::Key, usize>>,

    /// The configuration of `SyncFs`
    config: SyncFsConfig,
}

impl<T: Buffer> BufferUnit<T> {
    pub fn new(bid: usize, data: T) -> Self {
        Self {
            bid,
            refcnt: Mutex::new(0),
            data: RwLock::new(data),
        }
    }

    pub fn default(bid: usize) -> Self {
        Self {
            bid,
            refcnt: Mutex::new(0),
            data: Default::default(),
        }
    }

    pub fn key(&self) -> T::Key {
        self.data().key()
    }

    pub fn data(&self) -> RwLockReadGuard<'_, T> {
        self.data.read().unwrap()
    }

    pub fn data_mut(&self) -> RwLockWriteGuard<'_, T> {
        let mut guard = self.data.write().unwrap();
        guard.dirty();
        guard
    }
}

impl<'a, P: Pool> BufferHandle<'a, P> {
    /// It's safe when the provided wrapper is inited correctly.
    pub unsafe fn new(wrapper: &'a P::Unit, pool: &'a P) -> Self {
        pool.pin(wrapper);
        Self {
            unit: wrapper,
            pool,
        }
    }
}

impl<'a, P: Pool> Deref for BufferHandle<'a, P> {
    type Target = P::Unit;

    fn deref(&self) -> &Self::Target {
        self.unit
    }
}

impl<'a, P: Pool> Drop for BufferHandle<'a, P> {
    fn drop(&mut self) {
        self.pool.unpin(&self.unit);
    }
}

impl<K> Alteration<K>
where
    K: Hash + Eq,
{
    pub fn new() -> Self {
        Self {
            free: (0..BUFFER_POOL_SIZE).collect(),
            lru: LruCache::new(NonZeroUsize::new(BUFFER_POOL_SIZE).unwrap()),
        }
    }

    pub fn push_lru(&mut self, key: K, bid: usize) {
        self.lru.push(key, bid);
    }

    pub fn push_free(&mut self, bid: usize) {
        self.free.push(bid);
    }

    pub fn pop(&mut self) -> Result<usize, c_int> {
        if let Some(bid) = self.free.pop() {
            Ok(bid)
        } else {
            // look for lru
            if let Some((_, bid)) = self.lru.pop_lru() {
                Ok(bid)
            } else {
                Err(libc::ENOBUFS)
            }
        }
    }

    pub fn pop_lru(&mut self, key: &K) {
        self.lru.pop(key);
    }
}

impl<T: Buffer> BufferPool<T> {
    /// Please init `fs` field later.
    pub fn new(config: SyncFsConfig) -> Self {
        // create pool from beginning
        let pool: Vec<BufferUnit<T>> = (0..BUFFER_POOL_SIZE)
            .map(|bid| BufferUnit::default(bid))
            .collect();
        let pool = unsafe {
            Box::from_raw(
                Box::into_raw(pool.into_boxed_slice()) as *mut [BufferUnit<T>; BUFFER_POOL_SIZE]
            )
        }; // safe however

        Self {
            pool,
            alter: Mutex::new(Alteration::new()),
            map: RwLock::new(HashMap::new()),
            config,
        }
    }

    pub fn fetch(&self, key: &T::Key) -> Result<BufferHandle<Self>, c_int> {
        self.fetch_or_create(key, false)
    }

    pub fn create(&self, key: &T::Key) -> Result<BufferHandle<Self>, c_int> {
        self.fetch_or_create(key, true)
    }

    /// Return `None` only when there is no free buffer.
    fn fetch_or_create(&self, key: &T::Key, is_crt: bool) -> Result<BufferHandle<Self>, c_int> {
        loop {
            let bid = self.map.read().unwrap().get(key).map(|bid| *bid);
            if let Some(bid) = bid {
                let unit = &self.pool[bid];
                if unit.key().eq(key) {
                    return Ok(unsafe { BufferHandle::new(unit, self) });
                } else {
                    continue;
                }
            } else {
                let mut map_guard = self.map.write().unwrap();

                // check twice, because the lock had been released
                if let Some(bid) = map_guard.get(key) {
                    // map would always be locked first
                    let wrapper = &self.pool[*bid];
                    return Ok(unsafe { BufferHandle::new(wrapper, self) });
                } else {
                    let data = if is_crt {
                        T::empty(&self.config, key)
                    } else {
                        T::from_fs(&self.config, key)?
                    };

                    let bid = self.alter.lock().unwrap().pop()?;
                    map_guard.insert(key.clone(), bid);

                    // it would not be accessed concurrently
                    let unit = &self.pool[bid];
                    *unit.data_mut() = data;
                    return Ok(unsafe { BufferHandle::new(unit, self) });
                }
            }
        }
    }
}

impl<T: Buffer> Pool for BufferPool<T> {
    type Unit = BufferUnit<T>;

    /// Pin the buffer according to the given buffer id, make the `refcnt` increase by 1.
    ///
    /// The buffer would be removed from the LRU if the `refcnt == 0` before pinning.
    fn pin(&self, unit: &BufferUnit<T>) {
        let mut refcnt_guard = unit.refcnt.lock().unwrap();
        *refcnt_guard += 1;

        if *refcnt_guard == 1 {
            let key = &unit.key();
            self.alter.lock().unwrap().pop_lru(key);
        }
    }

    /// Unpin the buffer according to the given buffer id, make the `refcnt` decrease by 1.
    ///
    /// The buffer would put into "ready to be recycled" state if the `refcnt == 0` after unpinning.
    fn unpin(&self, unit: &BufferUnit<T>) {
        if self.config.is_direct() {
            unit.data_mut().fsync(&self.config);
        }

        let mut refcnt_guard = unit.refcnt.lock().unwrap();
        *refcnt_guard -= 1;

        if *refcnt_guard == 0 {
            let key = unit.key();
            let bid = unit.bid;
            self.alter.lock().unwrap().push_lru(key, bid);
        }
    }
}
