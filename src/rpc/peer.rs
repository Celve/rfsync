use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::fuse::server::SyncServer;

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct Peer {
    pub addr: SocketAddr,
    pub id: u64,
}

impl Peer {
    pub fn new(addr: SocketAddr, id: u64) -> Self {
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
