use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{path::RelPath, peer::Peer, remote::RemoteCell};

#[derive(Deserialize, Serialize)]
pub enum Request {
    ReadCell(RelPath),
    ReadFile(RelPath),
    SyncCell(Peer, RelPath),
}

#[derive(Deserialize, Serialize)]
pub enum Response {
    Cell(RemoteCell),
    #[serde(with = "serde_bytes")]
    File(Vec<u8>),
    Sync,
    Err(String),
}

#[derive(Debug)]
pub struct Comm {
    addr: SocketAddr,
}

impl Comm {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    pub async fn request(&self, req: &Request) -> Response {
        let mut stream = TcpStream::connect(self.addr).await.unwrap();

        // send request
        let req = bincode::serialize(&req).unwrap();
        stream.write(&req).await.unwrap();
        stream.shutdown().await.unwrap();

        // receive response
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        bincode::deserialize::<Response>(&buf).unwrap()
    }
}
