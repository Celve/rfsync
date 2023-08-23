use std::{
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use clap::Parser;
use fuser::MountOption;
use home::home_dir;
use log::LevelFilter;
use rfsync::{
    comm::{listener::Listener, peer::Peer},
    fuse::{fuse::SyncFuse, server::SyncServer},
};

const BUFFER_POOL_SIZE: usize = 1024;

#[derive(Parser)]
struct Cli {
    id: usize,
}

fn create_peer(id: usize) -> Peer {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080 + id as u16);
    Peer::new(addr, id)
}

fn main() {
    let cli = Cli::parse();
    let id = cli.id;

    env_logger::builder()
        .format_timestamp_nanos()
        .filter_level(LevelFilter::Debug)
        .init();

    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap(),
    );

    let db = home_dir().unwrap().join(".rfsync").join(id.to_string());
    let _ = fs::remove_dir_all(&db);

    let mut peers = vec![create_peer(0), create_peer(1)];
    let me = peers.remove(id);
    let srv = rt.block_on(SyncServer::new(me, db, true, peers));
    let fuse: SyncFuse<BUFFER_POOL_SIZE> = SyncFuse::new(rt.clone(), srv.clone());
    let listener = Listener::new(srv, me.addr);
    rt.spawn(listener.listen());

    let mut options = vec![MountOption::FSName("fuser".to_string())];
    options.push(MountOption::AutoUnmount);
    options.push(MountOption::AllowOther);

    let mountpoint = format!("./debug/{}", id);
    let _ = fs::remove_dir_all(&mountpoint);
    fs::create_dir_all(&mountpoint).unwrap();

    fuser::mount2(fuse, &mountpoint, &options).unwrap();
}
