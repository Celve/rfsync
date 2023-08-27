use std::{fmt::Debug, path::PathBuf};

use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::{
    comm::{
        iter::Iterator,
        oneway::{Oneway, Request, Response},
    },
    fuse::meta::FileTy,
    rsync::hashed::HashedList,
};

use super::{
    lean::LeanCelled,
    sync::{SyncCell, SyncCelled},
    time::VecTime,
};

#[derive(Deserialize, Serialize, Default)]
pub struct RemoteCell {
    /// The path of the file, relative to the root sync dir.
    pub(crate) path: PathBuf,

    /// The modification time vector.
    pub(crate) modif: VecTime,

    /// The synchronization time vector.
    pub(crate) sync: VecTime,

    /// The creationg time, which is the minimum value in the modification history.
    pub(crate) crt: usize,

    /// Indicate the type.
    pub(crate) ty: FileTy,

    /// Only use `PathBuf` because its children hasn't been fetched from remote.
    pub(crate) children: Vec<String>,

    pub(crate) list: HashedList,

    /// The remote server.
    /// When `RemoteCell` is inited as `default`, the client would be the local host.
    pub(crate) oneway: Oneway,
}

impl RemoteCell {
    pub async fn from_ow(path: PathBuf, oneway: Oneway) -> Self {
        let mut faucet = oneway.request(&Request::ReadCell(path)).await;
        let res = faucet.next().await.unwrap();

        if let Response::Cell(rc) = res {
            rc
        } else {
            Default::default()
        }
    }

    pub fn from_sc(sc: &SyncCell, oneway: Oneway) -> Self {
        RemoteCell {
            path: sc.path.clone(),
            modif: sc.modif.clone(),
            sync: sc.sync.clone(),
            crt: sc.crt.clone(),
            ty: sc.ty,
            children: sc.children.iter().map(|(name, _)| name.clone()).collect(),
            list: sc.list.clone(),
            oneway,
        }
    }

    pub fn empty(path: PathBuf) -> Self {
        Self {
            path,
            ..Default::default()
        }
    }

    pub async fn read_to_stream(
        &self,
        hashed_list: HashedList,
        mut file: impl AsyncWriteExt + Unpin,
    ) -> Option<RemoteCell> {
        let mut faucet = self
            .oneway
            .request(&Request::ReadFile(
                self.path.clone(),
                self.modif.clone(),
                hashed_list,
            ))
            .await;
        while let Some(reply) = faucet.next().await {
            if let Response::File(insts) = reply {
                let buf = bincode::serialize(&insts).unwrap();
                file.write_u64(buf.len() as u64).await.unwrap();
                file.write_all(&buf).await.unwrap();
            } else if let Response::Outdated(rc) = reply {
                return Some(rc);
            } else {
                panic!("")
            }
        }

        None
    }
}

impl LeanCelled for RemoteCell {
    fn modif(&self) -> &VecTime {
        &self.modif
    }

    fn sync(&self) -> &VecTime {
        &self.sync
    }

    fn path(&self) -> &PathBuf {
        &self.path
    }
}

impl SyncCelled for RemoteCell {
    fn crt(&self) -> usize {
        self.crt
    }

    fn ty(&self) -> FileTy {
        self.ty
    }

    fn list(&self) -> &HashedList {
        &self.list
    }
}

impl Debug for RemoteCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteCell")
            .field("path", &self.path)
            .field("modif", &self.modif)
            .field("sync", &self.sync)
            .field("ty", &self.ty)
            .finish()
    }
}
