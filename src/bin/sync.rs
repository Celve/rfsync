use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use clap::Parser;
use futures_util::future::join_all;
use rfsync::server::Server;
use tokio::time::{sleep_until, Instant};
use tracing::info;

#[derive(Parser)]
struct Cli {
    /// The path of directory to be synchronized
    path: PathBuf,

    /// The id of the server
    id: u16,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // register the subscriber
    let subscriber = tracing_subscriber::fmt().compact().finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    // init the server
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 60000 + cli.id);
    let server = Server::new(addr, cli.path, cli.id as usize).await;

    let handles = server.run();
    join_all(handles).await;
    info!("server {} is closed", cli.id);
}
