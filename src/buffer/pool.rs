use std::{collections::HashMap, fmt::Display, hash::Hash, ops::Deref, sync::Arc};

use libc::c_int;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{Mutex, RwLock};

use super::{
    alter::Alteration,
    disk::DiskManager,
    guard::{BufferReadGuard, BufferWriteGuard},
    unit::BufferUnit,
};

pub struct BufferPool<K, V, D, const S: usize>(Arc<BufferPoolInner<K, V, D, S>>)
where
    K: Default + Clone + Copy + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
    D: DiskManager<K, V>;

pub struct BufferPoolInner<K, V, D, const S: usize>
where
    K: Default + Clone + Copy + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
    D: DiskManager<K, V>,
{
    pub(super) units: Box<[BufferUnit<K, V>; S]>,
    alter: Mutex<Alteration<K, S>>,
    map: RwLock<HashMap<K, usize>>,
    dm: D,
    is_direct: bool,
}

impl<K, V, D, const S: usize> BufferPool<K, V, D, S>
where
    K: Default + Clone + Copy + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
    D: DiskManager<K, V>,
{
    pub fn new(dm: D, is_direct: bool) -> Self {
        // create pool from beginning
        let units: Vec<BufferUnit<K, V>> = (0..S).map(|bid| BufferUnit::new(bid)).collect();
        let units = unsafe {
            Box::from_raw(Box::into_raw(units.into_boxed_slice()) as *mut [BufferUnit<K, V>; S])
        }; // safe however

        Self(Arc::new(BufferPoolInner {
            units,
            alter: Mutex::new(Alteration::new()),
            map: RwLock::new(HashMap::new()),
            dm,
            is_direct,
        }))
    }

    pub async fn create(&self, key: &K) -> Result<BufferWriteGuard<K, V, D, S>, c_int> {
        let map_guard = self.map.read().await;
        let bid = map_guard.get(key);
        if let Some(bid) = bid {
            self.dm.create(key).await;
            *self.units[*bid].value.write().await = Default::default();
            Ok(BufferWriteGuard::new(*bid, &self).await)
        } else {
            drop(map_guard);
            let mut map_guard = self.map.write().await;

            // check twice
            if let Some(bid) = map_guard.get(key) {
                self.dm.create(key).await;
                *self.units[*bid].value.write().await = Default::default();
                Ok(BufferWriteGuard::new(*bid, &self).await)
            } else {
                // modify map
                let bid = self.alter.lock().await.pop()?;
                map_guard.insert(key.clone(), bid);

                // create on disk
                self.dm.create(key).await;

                // modify unit
                let unit = &self.units[bid];
                unit.reset(*key, Default::default(), &self.dm).await;

                Ok(BufferWriteGuard::new(bid, &self).await)
            }
        }
    }

    pub async fn read(&self, key: &K) -> Result<BufferReadGuard<K, V, D, S>, c_int> {
        let map_guard = self.map.read().await;
        let bid = map_guard.get(key);
        if let Some(bid) = bid {
            Ok(BufferReadGuard::new(*bid, &self).await)
        } else {
            drop(map_guard);
            let mut map_guard = self.map.write().await;

            // check twice
            if let Some(bid) = map_guard.get(key) {
                Ok(BufferReadGuard::new(*bid, &self).await)
            } else {
                // modify map
                let bid = self.alter.lock().await.pop()?;
                map_guard.insert(key.clone(), bid);

                // modify unit
                let unit = &self.units[bid];
                unit.reset(*key, self.dm.read(key).await, &self.dm).await;

                Ok(BufferReadGuard::new(bid, &self).await)
            }
        }
    }

    pub async fn write(&self, key: &K) -> Result<BufferWriteGuard<K, V, D, S>, c_int> {
        let map_guard = self.map.read().await;
        let bid = map_guard.get(key);
        if let Some(bid) = bid {
            Ok(BufferWriteGuard::new(*bid, &self).await)
        } else {
            drop(map_guard);
            let mut map_guard = self.map.write().await;

            // check twice
            if let Some(bid) = map_guard.get(key) {
                Ok(BufferWriteGuard::new(*bid, &self).await)
            } else {
                // modify map
                let bid = self.alter.lock().await.pop()?;
                map_guard.insert(key.clone(), bid);

                // modify unit
                let unit = &self.units[bid];
                unit.reset(*key, self.dm.read(key).await, &self.dm).await;

                Ok(BufferWriteGuard::new(bid, &self).await)
            }
        }
    }

    pub async fn pin(&self, bid: usize) {
        let unit = &self.units[bid];
        let mut refcnt_guard = unit.refcnt.lock().await;
        *refcnt_guard += 1;

        if *refcnt_guard == 1 {
            self.alter.lock().await.pop_lru(&unit.key().await);
        }
    }

    /// It should be guaranteed that the `BufferUnit` would not be accessed anymore after calling `unpin`.
    pub async fn unpin(&self, bid: usize) {
        let unit = &self.units[bid];
        let mut refcnt_guard = unit.refcnt.lock().await;
        *refcnt_guard -= 1;

        if *refcnt_guard == 0 {
            let key = unit.key().await;
            let bid = unit.bid;
            self.alter.lock().await.push_lru(key, bid);

            // for direct io
            if self.is_direct {
                unit.flush(&self.dm).await;
            }
        }
    }

    pub async fn release(&self, bid: usize) {
        let unit = &self.units[bid];
        let mut refcnt_guard = unit.refcnt.lock().await;
        *refcnt_guard -= 1;

        // although it's judged here, it should be guaranteed to be 0
        if *refcnt_guard == 0 {
            let key = unit.key().await;
            let bid = unit.bid;
            self.alter.lock().await.push_lru(key, bid);

            unit.remove(&self.dm).await;
        }
    }
}

impl<K, V, D, const S: usize> Deref for BufferPool<K, V, D, S>
where
    K: Default + Clone + Copy + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
    D: DiskManager<K, V>,
{
    type Target = BufferPoolInner<K, V, D, S>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K, V, D, const S: usize> Clone for BufferPool<K, V, D, S>
where
    K: Default + Clone + Copy + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
    D: DiskManager<K, V>,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
