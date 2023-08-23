use std::{fmt::Display, hash::Hash};

use serde::{de::DeserializeOwned, Serialize};

use super::{disk::DiskManager, pool::BufferPool};

pub struct BufferPin<'a, K, V, D, const S: usize>
where
    K: Default + Copy + Clone + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
    D: DiskManager<K, V>,
{
    pub(super) bid: usize,
    pub(super) pool: &'a BufferPool<K, V, D, S>,
    pub(super) is_release: bool,
}

impl<'a, K, V, D, const S: usize> BufferPin<'a, K, V, D, S>
where
    K: Default + Copy + Clone + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
    D: DiskManager<K, V>,
{
    pub async fn new(pool: &'a BufferPool<K, V, D, S>, bid: usize) -> Self {
        pool.pin(bid).await;
        Self {
            bid,
            pool,
            is_release: false,
        }
    }

    pub async fn release(mut self) {
        self.is_release = true;
        drop(self);
    }
}

impl<'a, K, V, D, const S: usize> Drop for BufferPin<'a, K, V, D, S>
where
    K: Default + Copy + Clone + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
    D: DiskManager<K, V>,
{
    fn drop(&mut self) {
        let pool = self.pool.clone();
        let bid = self.bid;
        if self.is_release {
            tokio::spawn(async move { pool.release(bid).await });
        } else {
            tokio::spawn(async move { pool.unpin(bid).await });
        }
    }
}
