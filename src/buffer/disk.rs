use std::fmt::Display;

use serde::{de::DeserializeOwned, Serialize};

pub trait DiskManager<K, V>: Send + Sync + 'static
where
    K: Display,
    V: DeserializeOwned + Serialize,
{
    async fn create(&self, key: &K);
    async fn read(&self, key: &K) -> V;
    async fn write(&self, key: &K, value: &V);
    async fn remove(&self, key: &K);
}
