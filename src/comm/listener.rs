use std::net::SocketAddr;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tracing::info;

use crate::{cell::remote::RemoteCell, fuse::server::SyncServer};

use super::oneway::{Oneway, Request, Response};

pub struct Listener<const S: usize> {
    srv: SyncServer<S>,
    addr: SocketAddr,
}

impl<const S: usize> Listener<S> {
    pub fn new(srv: SyncServer<S>, addr: SocketAddr) -> Self {
        Self { srv, addr }
    }

    pub async fn listen(self) {
        let listener = TcpListener::bind(self.addr).await.unwrap();

        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let srv = self.srv.clone();

            tokio::spawn(async move {
                let mut buf = Vec::new();
                let n = stream.read_to_end(&mut buf).await.unwrap();

                if n != 0 {
                    let req = bincode::deserialize::<Request>(&buf).unwrap();
                    match req {
                        Request::ReadCell(path) => {
                            let rc = if let Ok(sc) = srv.tree.read_by_path(&path).await {
                                RemoteCell::from_sc(&sc, Oneway::new(self.addr))
                            } else {
                                RemoteCell::empty(path)
                            };
                            info!("[rpc] send {:?}", rc);
                            stream
                                .write(&bincode::serialize(&Response::Cell(rc)).unwrap())
                                .await
                                .unwrap();
                        }
                        Request::ReadFile(path) => {
                            info!("[rpc] send file {:?}", &path);
                            let file = srv.read_file_by_path(&path).await;
                            let bytes = if let Ok(mut file) = file {
                                let mut bytes = Vec::new();
                                file.read_to_end(&mut bytes)
                                    .await
                                    .expect("read file failed");
                                bytes
                            } else {
                                Vec::new()
                            };
                            stream
                                .write(&bincode::serialize(&Response::File(bytes)).unwrap())
                                .await
                                .unwrap();
                        }
                        Request::SyncCell(peer, path) => {
                            info!("[rpc] asked to sync cell {:?}", &path);
                            let (ino, path) = srv.get_existing_ino_by_path(&path).await;
                            let res = srv
                                .sync(ino, RemoteCell::from_ow(path, peer.into()).await)
                                .await;
                            if res.is_ok() {
                                let res = bincode::serialize(&Response::Sync).unwrap();
                                stream.write(&res).await.unwrap();
                            } else {
                                panic!("fail to sync");
                            }
                        }
                    }
                    stream.shutdown().await.unwrap();
                } else {
                    println!("Find a empty connection");
                }
            });
        }
    }
}
