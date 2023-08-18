use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{
    comm::client::{Client, Request, Response},
    fuse::meta::FileTy,
};

use super::{sync::SyncCell, time::VecTime};

#[derive(Deserialize, Serialize, Default)]
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
    pub(super) ty: FileTy,

    /// Only use `PathBuf` because its children hasn't been fetched from remote.
    pub(super) children: Vec<String>,

    /// The remote server.
    /// When `RemoteCell` is inited as `default`, the client would be the local host.
    pub(super) cli: Client,
}

impl RemoteCell {
    pub async fn from_cli(path: PathBuf, cli: Client) -> Option<Self> {
        let res = cli.request(&Request::ReadCell(path)).await;

        if let Response::Cell(rc) = res {
            Some(rc)
        } else {
            None
        }
    }

    pub fn from_sc(sc: &SyncCell, cli: Client) -> Self {
        RemoteCell {
            path: sc.path.clone(),
            modif: sc.modif.clone(),
            sync: sc.sync.clone(),
            crt: sc.crt.clone(),
            ty: sc.ty,
            children: sc.children.iter().map(|(name, _)| name.clone()).collect(),
            cli,
        }
    }
}
