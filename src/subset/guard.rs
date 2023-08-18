use std::{fmt::Display, hash::Hash};

use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use super::subset::Subset;

pub struct SubsetReadGuard<'a, K, const S: usize>
where
    K: Default + Copy + Clone + Display + Hash + Eq + Send + Sync + 'static,
{
    bid: usize,
    _guard: RwLockReadGuard<'a, K>,
    subset: &'a Subset<K, S>,
}

pub struct SubsetWriteGuard<'a, K, const S: usize>
where
    K: Default + Copy + Clone + Display + Hash + Eq + Send + Sync + 'static,
{
    bid: usize,
    _guard: RwLockWriteGuard<'a, K>,
    subset: &'a Subset<K, S>,
}

impl<'a, K, const S: usize> SubsetReadGuard<'a, K, S>
where
    K: Default + Copy + Clone + Display + Hash + Eq + Send + Sync + 'static,
{
    pub async fn new(bid: usize, subset: &'a Subset<K, S>) -> Self {
        subset.pin(bid).await;
        Self {
            bid,
            _guard: subset.units[bid].key.read().await,
            subset,
        }
    }

    pub async fn upgrade(self) -> SubsetWriteGuard<'a, K, S> {
        let bid = self.bid;
        let subset = self.subset;
        subset.pin(bid).await;
        drop(self);

        SubsetWriteGuard {
            bid,
            _guard: subset.units[bid].key.write().await,
            subset,
        }
    }
}

impl<'a, K, const S: usize> SubsetWriteGuard<'a, K, S>
where
    K: Default + Copy + Clone + Display + Hash + Eq + Send + Sync + 'static,
{
    pub async fn new(bid: usize, subset: &'a Subset<K, S>) -> Self {
        subset.pin(bid).await;
        Self {
            bid,
            _guard: subset.units[bid].key.write().await,
            subset,
        }
    }
}

impl<'a, K, const S: usize> Drop for SubsetReadGuard<'a, K, S>
where
    K: Default + Copy + Clone + Display + Hash + Eq + Send + Sync + 'static,
{
    fn drop(&mut self) {
        let subset = self.subset.clone();
        let bid = self.bid;
        tokio::spawn(async move { subset.unpin(bid).await });
    }
}

impl<'a, K, const S: usize> Drop for SubsetWriteGuard<'a, K, S>
where
    K: Default + Copy + Clone + Display + Hash + Eq + Send + Sync + 'static,
{
    fn drop(&mut self) {
        let subset = self.subset.clone();
        let bid = self.bid;
        tokio::spawn(async move { subset.unpin(bid).await });
    }
}
