use std::{fmt::Debug, net::SocketAddr};

use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

use crate::{
    comm::{Comm, Request, Response},
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
    pub(super) addr: SocketAddr,
}

// utilities
impl RemoteCell {
    /// Read the file content from the remote server with TCP connections.
    pub async fn read_file(&self) -> Vec<u8> {
        let res = Comm::new(self.addr)
            .request(&Request::ReadFile(self.rel.clone()))
            .await;
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
            addr: remote,
        }
    }

    /// Read `RemoteCell` from remote server.
    #[instrument]
    pub async fn from_path(remote: SocketAddr, path: RelPath) -> Self {
        // establish connection
        let res = Comm::new(remote).request(&Request::ReadCell(path)).await;
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
            .field("ty", &self.ty)
            .finish()
    }
}
