use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::{
    cell::{remote::RemoteCell, time::VecTime},
    rsync::{hashed::HashedList, inst::Inst},
};

use super::{peer::Peer, reply::Replier};

#[derive(Deserialize, Serialize)]
pub enum MetaRequest {
    ReadCell(ReadCellRequest),
    ReadFile(ReadFileRequest),
    SyncCell(SyncCellRequest),
}

pub trait Request: DeserializeOwned + Serialize {
    type Response;

    fn to_meta_req(self) -> MetaRequest;
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ReadCellRequest {
    pub path: PathBuf,
}

impl ReadCellRequest {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Request for ReadCellRequest {
    type Response = RemoteCell;

    fn to_meta_req(self) -> MetaRequest {
        MetaRequest::ReadCell(self)
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ReadFileRequest {
    pub path: PathBuf,
    pub ver: VecTime,
    pub list: HashedList,
}

#[derive(Deserialize, Serialize)]
pub enum InstsOrRemoteCell {
    Insts(Vec<Inst>),
    RemoteCell(RemoteCell),
}

impl ReadFileRequest {
    pub fn new(path: PathBuf, ver: VecTime, list: HashedList) -> Self {
        Self { path, ver, list }
    }
}

impl Request for ReadFileRequest {
    type Response = InstsOrRemoteCell;

    fn to_meta_req(self) -> MetaRequest {
        MetaRequest::ReadFile(self)
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SyncCellRequest {
    pub peer: Peer,
    pub path: PathBuf,
}

impl SyncCellRequest {
    pub fn new(peer: Peer, path: PathBuf) -> Self {
        Self { peer, path }
    }
}

impl Request for SyncCellRequest {
    type Response = bool;

    fn to_meta_req(self) -> MetaRequest {
        MetaRequest::SyncCell(self)
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
pub struct Requestor {
    pub addr: SocketAddr,
}

impl Requestor {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    pub async fn request<R: Request>(&self, req: R) -> Replier<R::Response> {
        let mut stream = TcpStream::connect(self.addr).await.unwrap();

        // send request
        let req = bincode::serialize(&req.to_meta_req()).unwrap();
        stream.write(&req).await.unwrap();
        stream.shutdown().await.unwrap();

        Replier::new(stream)
    }
}

impl Default for Requestor {
    fn default() -> Self {
        Self {
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
        }
    }
}

impl From<Peer> for Requestor {
    fn from(value: Peer) -> Self {
        Self { addr: value.addr }
    }
}
