use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use tokio::{io::AsyncReadExt, net::TcpStream};

use super::iter::Iterator;

pub struct Faucet<T> {
    stream: TcpStream,
    data: PhantomData<T>,
}

impl<T> Faucet<T> {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            data: Default::default(),
        }
    }
}

impl<T> Iterator for Faucet<T>
where
    T: DeserializeOwned + Send + Sync,
{
    type Item = T;

    async fn next(&mut self) -> Option<T> {
        let res = self.stream.read_u64().await;
        match res {
            Ok(len) => {
                let mut buf = vec![0; len as usize];
                self.stream.read_exact(&mut buf).await.unwrap();
                Some(bincode::deserialize(&buf).unwrap())
            }

            Err(_) => None,
        }
    }
}
