use std::marker::PhantomData;

use serde::{de::DeserializeOwned, Serialize};
use tokio::{io::AsyncWriteExt, net::TcpStream};

pub struct Valve<T> {
    stream: TcpStream,
    data: PhantomData<T>,
}

impl<T> Valve<T>
where
    T: DeserializeOwned + Serialize + Send + Sync,
{
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            data: Default::default(),
        }
    }

    pub async fn send(&mut self, data: &T) {
        let buf = bincode::serialize(data).unwrap();
        let len = buf.len() as u64;
        self.stream.write_u64(len).await.unwrap();
        self.stream.write_all(&buf).await.unwrap();
    }

    pub async fn shutdown(mut self) {
        self.stream.shutdown().await.unwrap();
    }
}
