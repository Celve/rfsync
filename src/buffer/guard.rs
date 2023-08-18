use std::{
    fmt::Display,
    hash::Hash,
    ops::{Deref, DerefMut},
};

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use super::{disk::DiskManager, pool::BufferPool};

pub struct BufferReadGuard<'a, K, V, D, const S: usize>
where
    K: Default + Copy + Clone + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
    D: DiskManager<K, V>,
{
    bid: usize,
    value: RwLockReadGuard<'a, (bool, V)>,
    pool: &'a BufferPool<K, V, D, S>,
}

pub struct BufferWriteGuard<'a, K, V, D, const S: usize>
where
    K: Default + Copy + Clone + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
    D: DiskManager<K, V>,
{
    bid: usize,
    value: RwLockWriteGuard<'a, (bool, V)>,
    pool: &'a BufferPool<K, V, D, S>,
    pinned: bool,
}

impl<'a, K, V, D, const S: usize> BufferReadGuard<'a, K, V, D, S>
where
    K: Default + Copy + Clone + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
    D: DiskManager<K, V>,
{
    pub async fn new(bid: usize, pool: &'a BufferPool<K, V, D, S>) -> Self {
        pool.pin(bid).await;
        Self {
            bid,
            value: pool.units[bid].value.read().await,
            pool,
        }
    }

    pub async fn upgrade(self) -> BufferWriteGuard<'a, K, V, D, S> {
        let bid = self.bid;
        let pool = self.pool;
        pool.pin(bid).await;
        drop(self);

        let mut value = pool.units[bid].value.write().await;
        value.0 = true;

        BufferWriteGuard {
            bid,
            value,
            pool,
            pinned: true,
        }
    }
}

impl<'a, K, V, D, const S: usize> Drop for BufferReadGuard<'a, K, V, D, S>
where
    K: Default + Copy + Clone + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
    D: DiskManager<K, V>,
{
    fn drop(&mut self) {
        let pool = self.pool.clone();
        let bid = self.bid;
        tokio::spawn(async move { pool.unpin(bid).await });
    }
}

impl<'a, K, V, D, const S: usize> Deref for BufferReadGuard<'a, K, V, D, S>
where
    K: Default + Copy + Clone + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
    D: DiskManager<K, V>,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value.1
    }
}

impl<'a, K, V, D, const S: usize> BufferWriteGuard<'a, K, V, D, S>
where
    K: Default + Copy + Clone + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
    D: DiskManager<K, V>,
{
    pub async fn new(bid: usize, pool: &'a BufferPool<K, V, D, S>) -> Self {
        pool.pin(bid).await;

        let mut value = pool.units[bid].value.write().await;
        value.0 = true;

        Self {
            bid,
            value,
            pool,
            pinned: true,
        }
    }

    pub async fn downgrade(self) -> BufferReadGuard<'a, K, V, D, S> {
        let bid = self.bid;
        let pool = self.pool;
        pool.pin(bid).await;
        drop(self);

        let value = pool.units[bid].value.read().await;

        BufferReadGuard { bid, value, pool }
    }

    pub async fn destroy(mut self) {
        self.pool.release(self.bid).await;
        self.pinned = false;
    }
}

impl<'a, K, V, D, const S: usize> Drop for BufferWriteGuard<'a, K, V, D, S>
where
    K: Default + Copy + Clone + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
    D: DiskManager<K, V>,
{
    fn drop(&mut self) {
        if self.pinned {
            let pool = self.pool.clone();
            let bid = self.bid;
            tokio::spawn(async move { pool.unpin(bid).await });
        }
    }
}

impl<'a, K, V, D, const S: usize> Deref for BufferWriteGuard<'a, K, V, D, S>
where
    K: Default + Copy + Clone + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
    D: DiskManager<K, V>,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value.1
    }
}

impl<'a, K, V, D, const S: usize> DerefMut for BufferWriteGuard<'a, K, V, D, S>
where
    K: Default + Copy + Clone + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
    D: DiskManager<K, V>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value.1
    }
}
