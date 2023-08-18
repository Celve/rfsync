use std::{collections::HashMap, fmt::Display, hash::Hash, ops::Deref, sync::Arc};

use libc::c_int;
use tokio::sync::{Mutex, RwLock};

use crate::{buffer::alter::Alteration, subset::unit::SubsetUnit};

use super::guard::{SubsetReadGuard, SubsetWriteGuard};

#[derive()]
pub struct Subset<K, const S: usize>(Arc<SubsetInner<K, S>>)
where
    K: Default + Clone + Copy + Display + Hash + Eq + Send + Sync + 'static;

pub struct SubsetInner<K, const S: usize>
where
    K: Default + Clone + Copy + Display + Hash + Eq + Send + Sync + 'static,
{
    pub(super) units: Box<[SubsetUnit<K>; S]>,
    alter: Mutex<Alteration<K, S>>,
    map: RwLock<HashMap<K, usize>>,
}

impl<K, const S: usize> Subset<K, S>
where
    K: Default + Clone + Copy + Display + Hash + Eq + Send + Sync + 'static,
{
    pub fn new() -> Self {
        // create pool from beginning
        let units: Vec<SubsetUnit<K>> = (0..S).map(|bid| SubsetUnit::new(bid)).collect();
        let units = unsafe {
            Box::from_raw(Box::into_raw(units.into_boxed_slice()) as *mut [SubsetUnit<K>; S])
        }; // safe however

        Self(Arc::new(SubsetInner {
            units,
            alter: Mutex::new(Alteration::new()),
            map: RwLock::new(HashMap::new()),
        }))
    }

    pub async fn read(&self, key: &K) -> Result<SubsetReadGuard<K, S>, c_int> {
        let map_guard = self.map.read().await;
        let bid = map_guard.get(key);
        if let Some(bid) = bid {
            Ok(SubsetReadGuard::new(*bid, &self).await)
        } else {
            drop(map_guard);
            let mut map_guard = self.map.write().await;

            // check twice
            if let Some(bid) = map_guard.get(key) {
                Ok(SubsetReadGuard::new(*bid, &self).await)
            } else {
                // modify map
                let bid = self.alter.lock().await.pop()?;
                map_guard.insert(key.clone(), bid);

                // modify unit
                *self.units[bid].key.write().await = *key;

                Ok(SubsetReadGuard::new(bid, &self).await)
            }
        }
    }

    pub async fn write(&self, key: &K) -> Result<SubsetWriteGuard<K, S>, c_int> {
        let map_guard = self.map.read().await;
        let bid = map_guard.get(key);
        if let Some(bid) = bid {
            Ok(SubsetWriteGuard::new(*bid, &self).await)
        } else {
            drop(map_guard);
            let mut map_guard = self.map.write().await;

            // check twice
            if let Some(bid) = map_guard.get(key) {
                Ok(SubsetWriteGuard::new(*bid, &self).await)
            } else {
                // modify map
                let bid = self.alter.lock().await.pop()?;
                map_guard.insert(key.clone(), bid);

                // modify unit
                *self.units[bid].key.write().await = *key;

                Ok(SubsetWriteGuard::new(bid, &self).await)
            }
        }
    }

    pub async fn pin(&self, bid: usize) {
        let unit = &self.units[bid];
        let mut refcnt_guard = unit.refcnt.lock().await;
        *refcnt_guard += 1;

        if *refcnt_guard == 1 {
            self.alter
                .lock()
                .await
                .pop_lru(unit.key.read().await.deref());
        }
    }

    /// It should be guaranteed that the `SubsetUnit` would not be accessed anymore after calling `unpin`.
    pub async fn unpin(&self, bid: usize) {
        let unit = &self.units[bid];
        let mut refcnt_guard = unit.refcnt.lock().await;
        *refcnt_guard -= 1;

        if *refcnt_guard == 0 {
            let key = unit.key.read().await;
            let bid = unit.bid;
            self.alter.lock().await.push_lru(*key, bid);
        }
    }
}

impl<K, const S: usize> Deref for Subset<K, S>
where
    K: Default + Clone + Copy + Display + Hash + Eq + Send + Sync + 'static,
{
    type Target = SubsetInner<K, S>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K, const S: usize> Clone for Subset<K, S>
where
    K: Default + Clone + Copy + Display + Hash + Eq + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
