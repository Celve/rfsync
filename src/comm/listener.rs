use std::net::SocketAddr;

use tokio::{io::AsyncReadExt, net::TcpListener};
use tracing::info;

use crate::{
    cell::remote::RemoteCell,
    comm::iter::Iterator,
    fuse::server::SyncServer,
    rsync::{reconstruct::Reconstructor, table::HashTable},
};

use super::{
    oneway::{Oneway, Request, Response},
    valve::Valve,
};

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
                    let mut valve = Valve::new(stream);

                    let req = bincode::deserialize::<Request>(&buf).unwrap();
                    match req {
                        Request::ReadCell(path) => {
                            let rc = if let Ok(sc) = srv.tree.read_by_path(&path).await {
                                RemoteCell::from_sc(&sc, Oneway::new(self.addr))
                            } else {
                                RemoteCell::empty(path)
                            };
                            info!("[rpc] send {:?}", rc);
                            valve.send(&Response::Cell(rc)).await;
                        }

                        Request::ReadFile(path, ver, hashed_list) => {
                            info!("[rpc] send file {:?} with {:?}", &path, hashed_list);
                            let sc = srv.tree.read_by_path(&path).await.unwrap();
                            if sc.modif == ver {
                                let file = srv.read_file_by_path(&path).await;
                                if let Ok(mut file) = file {
                                    let hash_table = HashTable::new(&hashed_list);
                                    let mut reconstructor =
                                        Reconstructor::new(&hash_table, &mut file);
                                    while let Some(delta) = reconstructor.next().await {
                                        valve.send(&Response::File(delta)).await;
                                    }
                                } else {
                                    valve.send(&Response::File(Vec::new())).await;
                                };
                            } else {
                                info!("[rpc] outdated");
                                valve
                                    .send(&Response::Outdated(RemoteCell::from_sc(
                                        &sc,
                                        Oneway::new(self.addr),
                                    )))
                                    .await;
                            }
                        }

                        Request::SyncCell(peer, path) => {
                            info!("[rpc] asked to sync cell {:?}", &path);
                            let (ino, path) = srv.get_existing_ino_by_path(&path).await;
                            let res = srv
                                .sync(ino, RemoteCell::from_ow(path, peer.into()).await)
                                .await;
                            if res.is_ok() {
                                valve.send(&Response::Sync).await;
                            } else {
                                panic!("fail to sync");
                            }
                        }
                    }
                    valve.shutdown().await;
                } else {
                    println!("Find a empty connection");
                }
            });
        }
    }
}
