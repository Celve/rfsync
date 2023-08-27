use serde::{de::DeserializeOwned, Serialize};
use tokio::{io::AsyncWriteExt, net::TcpStream};

pub struct Repeater {
    stream: TcpStream,
}

impl Repeater {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    pub async fn send<T>(&mut self, data: &T)
    where
        T: DeserializeOwned + Serialize + Send + Sync,
    {
        let buf = bincode::serialize(data).unwrap();
        let len = buf.len() as u64;
        self.stream.write_u64(len).await.unwrap();
        self.stream.write_all(&buf).await.unwrap();
    }

    pub async fn shutdown(mut self) {
        self.stream.shutdown().await.unwrap();
    }
}
