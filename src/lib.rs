pub mod cell;
pub mod comm;
pub mod path;
pub mod peer;
pub mod server;
pub mod time;
pub mod watcher;

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        path::PathBuf,
        sync::Arc,
        time::Duration,
    };

    use futures_util::future::join_all;
    use tokio::{
        fs,
        task::JoinHandle,
        time::{sleep_until, Instant},
    };
    use tracing::info;

    use crate::{path::RootPath, peer::Peer, server::Server};

    const DEBUG_PATH: &str = "./debug";

    fn tmp_path() -> PathBuf {
        home::home_dir().unwrap().join(".rfsync")
    }

    fn server_path(id: usize) -> PathBuf {
        let path = PathBuf::from(DEBUG_PATH);
        path.join(id.to_string())
    }

    async fn generate_server(num: usize) -> (Vec<Arc<Server>>, Vec<JoinHandle<()>>) {
        let mut cluster = Vec::new();
        let mut peer_list = Vec::new();
        let mut handles = Vec::new();
        for id in 0..num {
            fs::create_dir(server_path(id)).await.unwrap();
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

    #[tokio::test]
    async fn two_server_sync() {
        // init the subscriber
        let subscriber = tracing_subscriber::fmt()
            .compact()
            .with_file(true)
            .with_line_number(true)
            .finish();
        tracing::subscriber::set_global_default(subscriber).unwrap();

        // remove the old
        if fs::metadata(DEBUG_PATH).await.is_ok() {
            fs::remove_dir_all(DEBUG_PATH).await.unwrap();
        }
        if fs::metadata(tmp_path()).await.is_ok() {
            fs::remove_dir_all(tmp_path()).await.unwrap();
        }

        fs::create_dir(DEBUG_PATH).await.unwrap();

        // make sure test env is cleared
        sleep_until(Instant::now() + Duration::from_millis(500)).await;

        // init two server
        let (_, handles) = generate_server(2).await;

        // make sure that all servers are inited
        sleep_until(Instant::now() + Duration::from_millis(500)).await;

        // test
        info!("create dir test in server 0");
        fs::create_dir(server_path(0).join("test")).await.unwrap();

        join_all(handles).await;
    }
}
