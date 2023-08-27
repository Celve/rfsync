use std::{fmt::Display, marker::PhantomData, path::PathBuf};

use async_trait::async_trait;
use tokio::fs::{self, File, OpenOptions};

use crate::buffer::disk::DiskManager;

#[derive(Clone)]
pub struct PrefixDirectDiskManager<K, V>
where
    K: Display + Send + Sync,
    V: FromIterator<u8> + Send + Sync,
    for<'a> &'a K: Send + Sync,
    for<'a> &'a V: IntoIterator<Item = &'a u8>,
{
    path: PathBuf,
    key: PhantomData<K>,
    value: PhantomData<V>,
}

impl<K, V> PrefixDirectDiskManager<K, V>
where
    K: Display + Send + Sync,
    V: FromIterator<u8> + Send + Sync,
    for<'a> &'a V: IntoIterator<Item = &'a u8>,
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

#[async_trait]
impl<K, V> DiskManager<K, V> for PrefixDirectDiskManager<K, V>
where
    K: Display + Send + Sync + 'static,
    V: FromIterator<u8> + Send + Sync + 'static,
    for<'a> &'a V: IntoIterator<Item = &'a u8>,
{
    async fn create(&self, key: &K) {
        File::create(&self.path(key)).await.unwrap();
    }

    async fn read(&self, key: &K) -> V {
        fs::read(&self.path(key))
            .await
            .expect("fail to read value from disk")
            .iter()
            .map(|byte| *byte)
            .collect()
    }

    async fn write(&self, key: &K, value: &V) {
        let bytes: Vec<u8> = value.into_iter().map(|byte| *byte).collect();
        fs::write(self.path(key), bytes)
            .await
            .expect("fail to write value to disk");
    }

    async fn remove(&self, key: &K) {
        fs::remove_file(&self.path(key))
            .await
            .expect("fail to remove value from disk");
    }

    async fn read_as_stream(&self, key: &K) -> File {
        OpenOptions::new()
            .read(true)
            .open(self.path(key))
            .await
            .expect("fail to read value from disk")
    }

    async fn write_as_stream(&self, key: &K) -> File {
        OpenOptions::new()
            .write(true)
            .open(self.path(key))
            .await
            .expect("fail to write value to disk")
    }
}
