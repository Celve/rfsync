use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};
use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::{
    cell::{remote::RemoteCell, time::VecTime},
    rsync::{hashed::HashedList, inst::Inst},
};

use super::{faucet::Faucet, peer::Peer};

#[derive(Deserialize, Serialize, Debug)]
pub enum Request {
    ReadCell(PathBuf),
    ReadFile(PathBuf, VecTime, HashedList),
    SyncCell(Peer, PathBuf),
}

#[derive(Deserialize, Serialize)]
pub enum Response {
    Cell(RemoteCell),
    File(Vec<Inst>),
    Sync,
    Outdated(RemoteCell),
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

    pub async fn request(&self, req: &Request) -> Faucet<Response> {
        let mut stream = TcpStream::connect(self.addr).await.unwrap();

        // send request
        let req = bincode::serialize(&req).unwrap();
        stream.write(&req).await.unwrap();
        stream.shutdown().await.unwrap();

        Faucet::new(stream)
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
