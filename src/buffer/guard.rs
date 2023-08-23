use std::{
    fmt::Display,
    hash::Hash,
    ops::{Deref, DerefMut},
};

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use super::{disk::DiskManager, pin::BufferPin};

pub struct BufferReadGuard<'a, K, V, D, const S: usize>
where
    K: Default + Copy + Clone + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
    D: DiskManager<K, V>,
{
    value: RwLockReadGuard<'a, (bool, V)>,
    pin: Option<BufferPin<'a, K, V, D, S>>,
}

pub struct BufferWriteGuard<'a, K, V, D, const S: usize>
where
    K: Default + Copy + Clone + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
    D: DiskManager<K, V>,
{
    value: RwLockWriteGuard<'a, (bool, V)>,
    pin: Option<BufferPin<'a, K, V, D, S>>,
}

impl<'a, K, V, D, const S: usize> BufferReadGuard<'a, K, V, D, S>
where
    K: Default + Copy + Clone + Display + Hash + Eq + Send + Sync + 'static,
    V: Default + DeserializeOwned + Serialize + Send + Sync + 'static,
    D: DiskManager<K, V>,
{
    pub async fn new(pin: BufferPin<'a, K, V, D, S>) -> Self {
        pin.pool.pin(pin.bid).await;
        Self {
            value: pin.pool.units[pin.bid].value.read().await,
            pin: Some(pin),
        }
    }

    pub async fn upgrade(mut self) -> BufferWriteGuard<'a, K, V, D, S> {
        let pin = self.pin.take().unwrap();
        drop(self);

        let mut value = pin.pool.units[pin.bid].value.write().await;
        value.0 = true;

        BufferWriteGuard {
            value,
            pin: Some(pin),
        }
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
    pub(super) async fn new(pin: BufferPin<'a, K, V, D, S>) -> Self {
        let mut value = pin.pool.units[pin.bid].value.write().await;
        value.0 = true;

        Self {
            value,
            pin: Some(pin),
        }
    }

    pub async fn downgrade(mut self) -> BufferReadGuard<'a, K, V, D, S> {
        let pin = self.pin.take().unwrap();
        drop(self);

        BufferReadGuard::new(pin).await
    }

    pub async fn destroy(mut self) {
        let pin = self.pin.take().unwrap();
        pin.release().await;
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
