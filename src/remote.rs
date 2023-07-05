use std::{net::SocketAddr, path::PathBuf};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{
    op::{Request, Response},
    sync::CellType,
    time::VecTime,
};

/// The representation of remote file/dir in memory,
/// whose children is updated through network request and reply.
#[derive(Deserialize, Serialize, Clone)]
pub struct RemoteCell {
    /// The path of the file, relative to the root sync dir.
    pub(super) path: PathBuf,

    /// The modification time vector.
    pub(super) modif: VecTime,

    /// The synchronization time vector.
    pub(super) sync: VecTime,

    /// The creationg time, which is the minimum value in the modification history.
    pub(super) crt: usize,

    /// Indicate the type.
    pub(super) ty: CellType,

    /// Only use `PathBuf` because its children hasn't been fetched from remote.
    pub(super) children: Vec<(PathBuf, CellType)>,

    /// The remote server.
    pub(super) remote: SocketAddr,
}

// utilities
impl RemoteCell {
    /// Read the file content from the remote server with TCP connections.
    pub async fn read_file(&self) -> Vec<u8> {
        let mut stream = TcpStream::connect(self.remote).await.unwrap();

        // send request
        let req = bincode::serialize(&Request::ReadFile(self.path.clone())).unwrap();
        stream.write(&req).await.unwrap();

        // receive response
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let res = bincode::deserialize::<Response>(&buf).unwrap();
        match res {
            Response::File(data) => data,
            _ => panic!("unexpected response"),
        }
    }

    pub fn is_existing(&self) -> bool {
        self.ty != CellType::None
    }
}

// constructors
impl RemoteCell {
    pub fn new(
        path: PathBuf,
        modif: VecTime,
        sync: VecTime,
        crt: usize,
        ty: CellType,
        children: Vec<(PathBuf, CellType)>,
        remote: SocketAddr,
    ) -> Self {
        Self {
            path,
            modif,
            sync,
            crt,
            ty,
            children,
            remote,
        }
    }

    /// Read `RemoteCell` from remote server.
    pub async fn from_path(remote: SocketAddr, path: PathBuf) -> Self {
        // establish connection
        let mut stream = TcpStream::connect(remote).await.unwrap();

        // send request
        let req = bincode::serialize(&Request::ReadCell(path)).unwrap();
        stream.write(&req).await.unwrap();

        // receive response
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let res = bincode::deserialize::<Response>(&buf).unwrap();
        match res {
            Response::Cell(cell) => cell,
            _ => panic!("unexpected response"),
        }
    }
}
