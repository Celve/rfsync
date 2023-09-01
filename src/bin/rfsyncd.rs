use std::{fs, net::SocketAddr, path::PathBuf, sync::Arc};

use clap::Parser;
use fuser::MountOption;
use home::home_dir;
use rfsync::{
    fuse::{fuse::SyncFuse, server::SyncServer},
    rpc::switch_server::SwitchServer,
};
use tonic::transport::Server;

const BUFFER_POOL_SIZE: usize = 1024;

#[derive(Parser)]
struct Cli {
    path: PathBuf,
    addr: SocketAddr,
    id: u64,
}

fn httped(addr: SocketAddr) -> String {
    format!("http://{}", addr)
}

fn main() {
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let cli = Cli::parse();
    let id = cli.id;
    let path = cli.path;
    let addr = cli.addr;

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap(),
    );

    let db = home_dir().unwrap().join(".rfsync").join(id.to_string());
    let _ = fs::remove_dir_all(&db);

    let srv = rt.block_on(SyncServer::new(id, httped(addr).to_string(), db, true));
    let srv_cloned = srv.clone();
    let fuse: SyncFuse<BUFFER_POOL_SIZE> = SyncFuse::new(rt.clone(), srv_cloned.clone());
    rt.spawn(async move {
        Server::builder()
            .add_service(SwitchServer::new(srv_cloned))
            .serve(addr.into())
            .await?;
        Ok::<(), tonic::transport::Error>(())
    });

    let mut options = vec![MountOption::FSName("fuser".to_string())];
    options.push(MountOption::AutoUnmount);
    options.push(MountOption::AllowOther);

    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).unwrap();

    fuser::mount2(fuse, &path, &options).unwrap();
}
