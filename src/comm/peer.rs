use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::fuse::server::SyncServer;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Peer {
    pub addr: SocketAddr,
    pub id: usize,
}

impl Peer {
    pub fn new(addr: SocketAddr, id: usize) -> Self {
        Self { addr, id }
    }
}

impl<const S: usize> From<&SyncServer<S>> for Peer {
    fn from(value: &SyncServer<S>) -> Self {
        Self {
            addr: value.addr(),
            id: value.mid(),
        }
    }
}
