use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::server::Server;

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

impl From<&Server> for Peer {
    fn from(value: &Server) -> Self {
        Self {
            addr: value.addr,
            id: value.id,
        }
    }
}
