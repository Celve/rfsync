use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::cell::remote::RemoteCell;

use super::peer::Peer;

#[derive(Deserialize, Serialize, Debug)]
pub enum Request {
    ReadCell(PathBuf),
    ReadFile(PathBuf),
    SyncCell(Peer, PathBuf),
}

#[derive(Deserialize, Serialize)]
pub enum Response {
    Cell(RemoteCell),
    #[serde(with = "serde_bytes")]
    File(Vec<u8>),
    Sync,
    Err(String),
}

#[derive(Clone, Copy, Deserialize, Serialize)]
pub struct Oneway {
    pub addr: SocketAddr,
}

impl Oneway {
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

impl Default for Oneway {
    fn default() -> Self {
        Self {
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
        }
    }
}

impl From<Peer> for Oneway {
    fn from(value: Peer) -> Self {
        Self { addr: value.addr }
    }
}
