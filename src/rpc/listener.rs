use std::net::SocketAddr;

use tokio::{io::AsyncReadExt, net::TcpListener};
use tracing::info;

use crate::{
    cell::remote::RemoteCell,
    fuse::server::SyncServer,
    rpc::{iter::Iterator, request::InstsOrRemoteCell},
    rsync::{reconstruct::Reconstructor, table::HashTable},
};

use super::{
    repeat::Repeater,
    request::{MetaRequest, Request, Requestor},
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
                    let mut repeater = Repeater::new(stream);

                    let req = bincode::deserialize::<MetaRequest>(&buf).unwrap();
                    match req {
                        MetaRequest::ReadCell(req) => {
                            let rc = if let Ok(sc) = srv.tree.read_by_path(&req.path).await {
                                RemoteCell::from_sc(&sc, Requestor::new(self.addr))
                            } else {
                                RemoteCell::empty(req.path)
                            };
                            info!("[rpc] send {:?}", rc);
                            repeater.send(&rc).await;
                        }

                        MetaRequest::ReadFile(req) => {
                            info!("[rpc] send file {:?} with {:?}", &req.path, req.list);
                            let sc = srv.tree.read_by_path(&req.path).await.unwrap();
                            if sc.modif == req.ver {
                                let file = srv.read_file_by_path(&req.path).await;
                                if let Ok(mut file) = file {
                                    let hash_table = HashTable::new(&req.list);
                                    let mut reconstructor =
                                        Reconstructor::new(&hash_table, &mut file);
                                    while let Some(delta) = reconstructor.next().await {
                                        repeater.send(&InstsOrRemoteCell::Insts(delta)).await;
                                    }
                                } else {
                                    repeater.send(&InstsOrRemoteCell::Insts(Vec::new())).await;
                                };
                            } else {
                                info!("[rpc] outdated");
                                repeater
                                    .send(&InstsOrRemoteCell::RemoteCell(RemoteCell::from_sc(
                                        &sc,
                                        Requestor::new(self.addr),
                                    )))
                                    .await;
                            }
                        }

                        MetaRequest::SyncCell(req) => {
                            info!("[rpc] asked to sync cell {:?}", &req.path);
                            let (ino, path) = srv.get_existing_ino_by_path(&req.path).await;
                            let res = srv
                                .sync(ino, RemoteCell::from_ow(path, req.peer.into()).await)
                                .await;
                            repeater.send(&res.is_ok()).await;
                        }
                    }
                    repeater.shutdown().await;
                } else {
                    println!("Find a empty connection");
                }
            });
        }
    }
}
