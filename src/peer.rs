use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct Peer {
    pub addr: SocketAddr,
    pub id: usize,
}

#[derive(Clone)]
pub struct PeerList {
    pub peers: Vec<Peer>,
}

impl Peer {
    pub fn new(addr: SocketAddr, id: usize) -> Self {
        Self { addr, id }
    }
}

impl PeerList {
    pub fn new() -> Self {
        Self { peers: Vec::new() }
    }

    pub fn push(&mut self, peer: Peer) {
        self.peers.push(peer);
    }
}
