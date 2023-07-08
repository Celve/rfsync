use std::{fmt::Debug, net::SocketAddr};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing::{info, instrument};

use crate::{
    op::{Request, Response},
    path::RelPath,
    sync::CellType,
    time::VecTime,
};

/// The representation of remote file/dir in memory,
/// whose children is updated through network request and reply.
#[derive(Deserialize, Serialize, Clone)]
pub struct RemoteCell {
    /// The path of the file, relative to the root sync dir.
    pub(super) rel: RelPath,

    /// The modification time vector.
    pub(super) modif: VecTime,

    /// The synchronization time vector.
    pub(super) sync: VecTime,

    /// The creationg time, which is the minimum value in the modification history.
    pub(super) crt: usize,

    /// Indicate the type.
    pub(super) ty: CellType,

    /// Only use `PathBuf` because its children hasn't been fetched from remote.
    pub(super) children: Vec<(RelPath, CellType)>,

    /// The remote server.
    pub(super) remote: SocketAddr,
}

// utilities
impl RemoteCell {
    /// Read the file content from the remote server with TCP connections.
    pub async fn read_file(&self) -> Vec<u8> {
        let mut stream = TcpStream::connect(self.remote).await.unwrap();

        // send request
        let req = bincode::serialize(&Request::ReadFile(self.rel.clone())).unwrap();
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
        path: RelPath,
        modif: VecTime,
        sync: VecTime,
        crt: usize,
        ty: CellType,
        children: Vec<(RelPath, CellType)>,
        remote: SocketAddr,
    ) -> Self {
        Self {
            rel: path,
            modif,
            sync,
            crt,
            ty,
            children,
            remote,
        }
    }

    /// Read `RemoteCell` from remote server.
    #[instrument]
    pub async fn from_path(remote: SocketAddr, path: RelPath) -> Self {
        // establish connection
        let mut stream = TcpStream::connect(remote).await.unwrap();

        // send request
        let req = bincode::serialize(&Request::ReadCell(path)).unwrap();
        stream.write(&req).await.unwrap();
        stream.shutdown().await.unwrap();

        // receive response
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let res = bincode::deserialize::<Response>(&buf).unwrap();
        match res {
            Response::Cell(cell) => {
                info!("make {:?}", cell);
                cell
            }
            _ => panic!("unexpected response"),
        }
    }
}

impl Debug for RemoteCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteCell")
            .field("rel", &self.rel)
            .field("modif", &self.modif)
            .field("sync", &self.sync)
            .finish()
    }
}
