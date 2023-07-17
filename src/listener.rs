use std::{net::SocketAddr, sync::Weak};

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tracing::info;

use crate::{
    comm::{Request, Response},
    server::Server,
};

pub struct Listener {
    addr: SocketAddr,
    srv: Weak<Server>,
}

impl Listener {
    pub fn new(addr: SocketAddr, srv: Weak<Server>) -> Self {
        Self { addr, srv }
    }

    pub async fn listen(self) {
        let listener = TcpListener::bind(self.addr).await.unwrap();

        info!("listener started");

        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let srv = self.srv.clone();

            tokio::spawn(async move {
                let mut buf = Vec::new();
                let n = stream.read_to_end(&mut buf).await.unwrap();

                if n != 0 {
                    let req = bincode::deserialize::<Request>(&buf).unwrap();
                    let server = srv.upgrade().unwrap();
                    info!("{:?} recvs {:?}", server, req);
                    match req {
                        Request::ReadCell(path) => {
                            let sc = server.get_sc(&path).await.expect("there should be one tc");
                            let res = Response::Cell(sc.into_rc(server.addr).await);
                            let res = bincode::serialize(&res).unwrap();
                            stream.write(&res).await.unwrap();
                        }
                        Request::ReadFile(path) => {
                            // TODO: I should not use unwrap here
                            let path = &server.root + &path;
                            let mut file = File::open(path.as_path_buf()).await.unwrap();

                            // TODO: use frame to optimize
                            let mut buf = Vec::new();
                            file.read_to_end(&mut buf).await.unwrap();
                            let res = Response::File(buf);
                            let res = bincode::serialize(&res).unwrap();
                            stream.write(&res).await.unwrap();
                        }
                        Request::SyncCell(peer, path) => {
                            let sc = server.make_sc(&path).await;
                            sc.sync(peer.addr).await;
                            let res = bincode::serialize(&Response::Sync).unwrap();
                            stream.write(&res).await.unwrap();
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
