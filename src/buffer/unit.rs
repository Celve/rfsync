use std::{fmt::Display, hash::Hash};

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{Mutex, RwLock};

use super::disk::DiskManager;

pub struct BufferUnit<K, V>
where
    K: Default + Clone + Copy + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
{
    pub(super) bid: usize,

    /// `key` could only be reset with the reset function.
    key: RwLock<K>,
    pub(super) value: RwLock<(bool, V)>,

    /// The `refcnt` should not be replaced by `AtomicUsize`,
    /// because critical section is required when increaing or decreasing it.
    pub(super) refcnt: Mutex<usize>,
}

impl<K, V> BufferUnit<K, V>
where
    K: Default + Clone + Copy + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
{
    pub fn new(bid: usize) -> Self {
        Self {
            bid,
            key: RwLock::new(Default::default()),
            value: RwLock::new(Default::default()),
            refcnt: Mutex::new(0),
        }
    }

    pub async fn key(&self) -> K {
        *self.key.read().await
    }

    /// Whether to flush or not is fully depends on the operations of `BufferPool`.
    /// For `BufferUnit`, it does nothing to do with flushing.
    pub async fn reset(&self, key: K, value: V, dm: &impl DiskManager<K, V>) {
        let mut key_guard = self.key.write().await;
        let mut value_guard = self.value.write().await;
        if value_guard.0 {
            // dm.write(&key_guard, &value_guard.1).await;
            dm.write(&key_guard, &value_guard.1).await;
        }

        // update
        *key_guard = key;
        *value_guard = (false, value);
    }

    pub async fn try_flush(&self, dm: &impl DiskManager<K, V>) {
        let refcnt_guard = self.refcnt.lock().await;
        if *refcnt_guard == 0 {
            self.flush(dm).await;
        }
    }

    /// Please call this function when the `refcnt` is locked.
    /// Because `refcnt` is used to demarcate the critical section.
    pub async fn flush(&self, dm: &impl DiskManager<K, V>) {
        let value_guard = self.value.read().await;
        if value_guard.0 {
            drop(value_guard);
            // always lock the key first
            let key_guard = self.key.read().await;
            let mut value_guard = self.value.write().await;

            if value_guard.0 {
                dm.write(&key_guard, &value_guard.1).await;
                value_guard.0 = false;
            }
        }
    }

    pub async fn remove(&self, dm: &impl DiskManager<K, V>) {
        let key_guard = self.key.read().await;
        dm.remove(&key_guard).await;
    }
}
