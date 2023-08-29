use std::{fmt::Debug, path::PathBuf};

use tokio::io::AsyncWriteExt;
use tonic::transport::Channel;

use crate::{
    fuse::meta::FileTy,
    rpc::{
        read_file_reply::InstOrRemoteCell, switch_client::SwitchClient, FakeFileTy, FakeRemoteCell,
        ReadCellRequest, ReadFileRequest,
    },
    rsync::{
        hashed::{Hashed, HashedList},
        inst::Inst,
    },
};

use super::{lean::LeanCelled, sync::SyncCelled, time::VecTime};

pub struct RemoteCell {
    /// The path of the file, relative to the root sync dir.
    pub(crate) path: PathBuf,

    /// The modification time vector.
    pub(crate) modif: VecTime,

    /// The synchronization time vector.
    pub(crate) sync: VecTime,

    /// The creationg time, which is the minimum value in the modification history.
    pub(crate) crt: u64,

    /// Indicate the type.
    pub(crate) ty: FileTy,

    /// Only use `PathBuf` because its children hasn't been fetched from remote.
    pub(crate) children: Vec<String>,

    pub(crate) list: HashedList,

    /// The remote server.
    /// When `RemoteCell` is inited as `default`, the client would be the local host.
    pub(crate) client: SwitchClient<Channel>,
}

impl RemoteCell {
    fn from_faked(value: FakeRemoteCell, client: SwitchClient<Channel>) -> Self {
        Self {
            path: value.path.into(),
            modif: (&value.modif).into(),
            sync: (&value.sync).into(),
            crt: value.crt,
            ty: FakeFileTy::from_i32(value.ty).unwrap().into(),
            children: value.children,
            list: value
                .list
                .iter()
                .map(|h| Hashed::from(h))
                .collect::<Vec<_>>()
                .into(),
            client,
        }
    }

    pub async fn from_client(client: &mut SwitchClient<Channel>, path: PathBuf) -> Self {
        let req = ReadCellRequest {
            path: path.to_string_lossy().to_string(),
        };

        Self::from_faked(
            client
                .read_cell(req)
                .await
                .expect("fail to read cell")
                .into_inner()
                .rc
                .unwrap(),
            client.clone(),
        )
    }

    pub async fn read_to_stream(
        &mut self,
        list: &HashedList,
        mut file: impl AsyncWriteExt + Unpin,
    ) -> Option<RemoteCell> {
        let req = ReadFileRequest {
            path: self.path.to_string_lossy().to_string(),
            ver: (&self.modif).into(),
            list: list.into(),
        };

        let mut res = self
            .client
            .read_file(req)
            .await
            .expect("fail to read file")
            .into_inner();

        while let Some(reply) = res.message().await.expect("fail when messaging") {
            let ior = reply.inst_or_remote_cell.unwrap();
            match ior {
                InstOrRemoteCell::Inst(inst) => {
                    let inst = Inst::from(inst);
                    let buf = bincode::serialize(&inst).unwrap();
                    file.write_u64(buf.len() as u64).await.unwrap();
                    file.write_all(&buf).await.unwrap();
                }
                InstOrRemoteCell::Cell(rc) => {
                    return Some(Self::from_faked(rc, self.client.clone()));
                }
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
    fn crt(&self) -> u64 {
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
