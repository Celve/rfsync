use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};

use clap::Parser;
use rfsync::{
    path::RootPath,
    peer::{Peer, PeerList},
    server::Server,
};
use tracing::info;

#[derive(Parser)]
struct Cli {
    /// The path of directory to be synchronized
    path: PathBuf,

    /// The id of the server
    id: u16,
}

const PEER_NUM: usize = 10;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // init the peer list
    let mut peer_list = PeerList::new();
    for i in 0..PEER_NUM {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 60000 + cli.id);
        peer_list.push(Peer::new(addr, i));
    }

    // register the subscriber
    let subscriber = tracing_subscriber::fmt().compact().finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    // init the server
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 60000 + cli.id);
    let path = RootPath::new(cli.path);
    let server = Server::new(addr, &path, cli.id as usize).await;
    *server.peers.write().await = peer_list;
    server.clone().init().await;

    server.run().await.unwrap();
    info!("server {} is closed", cli.id);
}
