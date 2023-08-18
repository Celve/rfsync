use std::{
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use fuser::MountOption;
use home::home_dir;
use log::LevelFilter;
use rfsync::fuse::fuse::SyncFuse;

const BUFFER_POOL_SIZE: usize = 1024;

fn main() {
    env_logger::builder()
        .format_timestamp_nanos()
        .filter_level(LevelFilter::Debug)
        .init();

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap(),
    );

    let db = home_dir().unwrap().join(".rfsync");
    fs::remove_dir_all(&db).unwrap();

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    let fuse: SyncFuse<BUFFER_POOL_SIZE> = SyncFuse::new(rt, 0, db, true, addr);

    let mut options = vec![MountOption::FSName("fuser".to_string())];
    options.push(MountOption::AutoUnmount);
    options.push(MountOption::AllowOther);

    let mountpoint = "./debug";

    fuser::mount2(fuse, &mountpoint, &options).unwrap();
}
