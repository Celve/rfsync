use std::{
    error::Error,
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use clap::Parser;
use fuser::MountOption;
use home::home_dir;
use log::LevelFilter;
use rfsync::{
    fuse::{fuse::SyncFuse, server::SyncServer},
    rpc::switch_server::SwitchServer,
};
use tokio::io::{stdin, AsyncBufReadExt, BufReader};
use tonic::transport::Server;
use tracing::info;

const BUFFER_POOL_SIZE: usize = 1024;

#[derive(Parser)]
struct Cli {
    id: u64,
}

fn create_addr(id: u64) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080 + id as u16)
}

fn create_http(id: u64) -> String {
    format!("http://{}", create_addr(id))
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let id = cli.id;

    // env_logger::builder()
    //     .format_timestamp_nanos()
    //     .filter_level(LevelFilter::Debug)
    //     .init();

    // let subscriber = tracing_subscriber::fmt()
    //     .compact()
    //     .with_file(true)
    //     .with_line_number(true)
    //     .finish();
    // tracing::subscriber::set_global_default(subscriber).unwrap();

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap(),
    );

    let db = home_dir().unwrap().join(".rfsync").join(id.to_string());
    let _ = fs::remove_dir_all(&db);

    let srv = rt.block_on(SyncServer::new(id, create_http(id).to_string(), db, true));
    let srv_cloned = srv.clone();
    let fuse: SyncFuse<BUFFER_POOL_SIZE> = SyncFuse::new(rt.clone(), srv_cloned.clone());
    rt.spawn(async move {
        Server::builder()
            .add_service(SwitchServer::new(srv_cloned))
            .serve(create_addr(id))
            .await?;
        Ok::<(), tonic::transport::Error>(())
    });

    let mut options = vec![MountOption::FSName("fuser".to_string())];
    options.push(MountOption::AutoUnmount);
    options.push(MountOption::AllowOther);

    let mountpoint = format!("./debug/{}", id);
    let _ = fs::remove_dir_all(&mountpoint);
    fs::create_dir_all(&mountpoint).unwrap();

    rt.spawn(async move {
        let mut lines = BufReader::new(stdin()).lines();
        while let Some(input) = lines.next_line().await? {
            let id = input.parse::<u64>().unwrap();
            srv.join_server(create_http(id).to_string()).await;
        }

        Ok::<(), tokio::io::Error>(())
    });

    fuser::mount2(fuse, &mountpoint, &options).unwrap();

    Ok(())
}
