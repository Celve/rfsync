use std::{fmt::Display, marker::PhantomData, path::PathBuf};

use serde::{de::DeserializeOwned, Serialize};
use tokio::fs::{self, File};

use crate::buffer::disk::DiskManager;

#[derive(Clone)]
pub struct PrefixSerdeDiskManager<K, V>
where
    K: Display + Send + Sync,
    V: DeserializeOwned + Serialize + Send + Sync,
{
    path: PathBuf,
    key: PhantomData<K>,
    value: PhantomData<V>,
}

impl<K, V> PrefixSerdeDiskManager<K, V>
where
    K: Display + Send + Sync,
    V: DeserializeOwned + Serialize + Send + Sync,
{
    pub async fn new(path: PathBuf) -> Self {
        fs::create_dir_all(&path).await.unwrap();
        Self {
            path,
            key: PhantomData,
            value: PhantomData,
        }
    }

    pub fn path(&self, k: &K) -> PathBuf {
        self.path.join(k.to_string())
    }
}

impl<K, V> DiskManager<K, V> for PrefixSerdeDiskManager<K, V>
where
    K: Display + Send + Sync + 'static,
    V: DeserializeOwned + Serialize + Send + Sync + 'static,
{
    async fn create(&self, key: &K) {
        File::create(&self.path(key)).await.unwrap();
    }

    async fn read(&self, key: &K) -> V {
        let bytes = fs::read(&self.path(key))
            .await
            .expect("fail to read value from disk");
        bincode::deserialize(&bytes)
            .expect(&format!("fail to deserialize value from disk with {}", key))
    }

    async fn write(&self, key: &K, value: &V) {
        fs::write(
            self.path(key),
            bincode::serialize(value).expect("fail to write value to disk"),
        )
        .await
        .expect("fail to write value to disk");
    }

    async fn remove(&self, key: &K) {
        fs::remove_file(&self.path(key))
            .await
            .expect("fail to remove value from disk");
    }
}