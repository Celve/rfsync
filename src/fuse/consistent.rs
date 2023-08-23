use std::{ops::Deref, path::PathBuf};

use serde::{de::DeserializeOwned, Serialize};
use tokio::{fs, sync::Mutex};

pub struct Consistent<T>
where
    T: DeserializeOwned + Serialize + Clone + Copy,
{
    path: PathBuf,
    data: Mutex<T>,
}

impl<T> Consistent<T>
where
    T: DeserializeOwned + Serialize + Clone + Copy,
{
    /// If there is already an file on the disk, read the value.
    /// Otherwise use the provided data instead.
    pub async fn new(path: PathBuf, default: T) -> Self {
        let data = if let Ok(bytes) = fs::read(&path).await {
            bincode::deserialize(&bytes).expect("fail to deserialize consistent file")
        } else {
            fs::write(&path, bincode::serialize(&default).unwrap())
                .await
                .unwrap();
            default
        };

        Self {
            path,
            data: Mutex::new(data),
        }
    }

    pub async fn apply(&self, f: fn(T) -> T) -> T {
        let mut guard = self.data.lock().await;
        let original = *guard;
        *guard = f(*guard);

        // make it consistent
        fs::write(&self.path, bincode::serialize(guard.deref()).unwrap())
            .await
            .unwrap();

        original
    }
}
