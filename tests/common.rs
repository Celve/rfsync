use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
};

use rfsync::{path::RootPath, peer::Peer, server::Server};
use tokio::{fs, task::JoinHandle};

pub fn src_path() -> PathBuf {
    PathBuf::from("./debug")
}

pub fn tmp_path() -> PathBuf {
    home::home_dir().unwrap().join(".rfsync")
}

pub fn server_path(id: usize) -> PathBuf {
    src_path().join(id.to_string())
}

pub async fn init_tmp_dirs() {
    if fs::metadata(tmp_path()).await.is_ok() {
        fs::remove_dir_all(tmp_path()).await.unwrap();
    }
}

pub async fn init_src_dirs(num_server: usize) {
    if fs::metadata(src_path()).await.is_ok() {
        fs::remove_dir_all(src_path()).await.unwrap();
    }

    fs::create_dir(src_path()).await.unwrap();

    for id in 0..num_server {
        fs::create_dir_all(server_path(id)).await.unwrap();
    }
}

pub async fn generate_server(num: usize) -> (Vec<Arc<Server>>, Vec<JoinHandle<()>>) {
    let mut cluster = Vec::new();
    let mut peer_list = Vec::new();
    let mut handles = Vec::new();
    for id in 0..num {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 60000 + id as u16);
        let server = Server::new(addr, &RootPath::new(server_path(id)), id as usize).await;
        handles.extend(server.run());
        cluster.push(server);
        peer_list.push(Peer::new(addr, id as usize));
    }

    for server in cluster.iter() {
        *server.peers.write().await = peer_list.clone();
    }

    (cluster, handles)
}
