use std::{collections::VecDeque, fmt::Display, path::PathBuf};

use async_recursion::async_recursion;
use futures_util::future::join_all;
use libc::c_int;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::{fuse::meta::FileTy, rpc::oneway::Oneway, rsync::hashed::HashedList};

use super::{
    lean::LeanCelled, remote::RemoteCell, stge::CopyStge, sync::SyncCelled, time::VecTime,
    tree::SyncTree,
};

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncOp {
    #[default]
    None,
    Copy,
    Conflict,
    Recurse,
}

pub struct CopyCell {
    /// The unique id for `CopyCell`.
    pub(crate) cid: u64,

    /// Find the correspond `SyncCell` by `sid`.
    pub(crate) sid: u64,

    /// Further fetch the `RemoteCell`.
    pub(crate) oneway: Oneway,

    pub(crate) stge: CopyStge,

    /// The supposed synchronization operation.
    pub(crate) sop: SyncOp,

    pub(crate) ver: VecTime,

    pub(crate) path: PathBuf,
    pub(crate) modif: VecTime,
    pub(crate) sync: VecTime,
    pub(crate) crt: usize,
    pub(crate) ty: FileTy,
    pub(crate) children: Vec<(String, CopyCell)>,
    pub(crate) list: HashedList,
}

impl CopyCell {
    /// Only `children` is inited as empty,
    /// because the children provided by `RemoteCell` would always not be the children that we wanna sync.
    async fn from_rc(
        cid: u64,
        sid: u64,
        rc: &RemoteCell,
        sop: SyncOp,
        ver: VecTime,
        children: Vec<(String, CopyCell)>,
        stge: CopyStge,
    ) -> CopyCell {
        CopyCell {
            cid,
            sid,
            oneway: rc.oneway,
            stge,
            sop,
            ver,
            path: rc.path.clone(),
            modif: rc.modif.clone(),
            sync: rc.sync.clone(),
            crt: rc.crt,
            ty: rc.ty,
            list: rc.list.clone(),
            children,
        }
    }

    #[async_recursion]
    pub async fn make<const S: usize>(
        sid: u64,
        rc: RemoteCell,
        tree: SyncTree<S>,
        stge: CopyStge,
    ) -> Result<CopyCell, c_int> {
        let sc = tree.read_by_id(&sid).await?;
        let sop = sc.calc_sync_op(&rc);
        drop(sc);

        Ok(match sop {
            SyncOp::None => Self::none(sid, &rc, tree, stge).await?,
            SyncOp::Copy => Self::copy(sid, &rc, tree, stge).await?,
            SyncOp::Conflict => Self::conflict(sid, &rc, tree, stge).await?,
            SyncOp::Recurse => Self::recurse(sid, &rc, tree, stge).await?,
        })
    }

    pub async fn none<const S: usize>(
        sid: u64,
        rc: &RemoteCell,
        tree: SyncTree<S>,
        stge: CopyStge,
    ) -> Result<CopyCell, c_int> {
        let sc = tree.read_by_id(&sid).await.unwrap();
        Ok(CopyCell::from_rc(
            stge.alloc_cid(),
            sid,
            rc,
            SyncOp::None,
            sc.modif.clone(),
            Vec::new(),
            stge,
        )
        .await)
    }

    pub async fn copy<const S: usize>(
        sid: u64,
        rc: &RemoteCell,
        tree: SyncTree<S>,
        stge: CopyStge,
    ) -> Result<CopyCell, c_int> {
        let sc = tree.read_by_id(&sid).await?;
        let cid = stge.alloc_cid();
        stge.create(&cid).await;
        if let Some(rc) = rc
            .read_to_stream(sc.list.clone(), stge.write_as_stream(&cid).await)
            .await
        {
            drop(sc);
            Self::make(sid, rc, tree, stge).await
        } else {
            Ok(CopyCell::from_rc(
                cid,
                sid,
                rc,
                SyncOp::Copy,
                sc.modif.clone(),
                Vec::new(),
                stge,
            )
            .await)
        }
    }

    pub async fn conflict<const S: usize>(
        sid: u64,
        rc: &RemoteCell,
        tree: SyncTree<S>,
        stge: CopyStge,
    ) -> Result<CopyCell, c_int> {
        let sc = tree.read_by_id(&sid).await?;
        let cid = stge.alloc_cid();
        stge.create(&cid).await;
        if let Some(rc) = rc
            .read_to_stream(sc.list.clone(), stge.write_as_stream(&cid).await)
            .await
        {
            drop(sc);
            Self::make(sid, rc, tree, stge).await
        } else {
            Ok(CopyCell::from_rc(
                cid,
                sid,
                rc,
                SyncOp::Conflict,
                sc.modif.clone(),
                Vec::new(),
                stge,
            )
            .await)
        }
    }

    pub async fn recurse<const S: usize>(
        sid: u64,
        rc: &RemoteCell,
        tree: SyncTree<S>,
        stge: CopyStge,
    ) -> Result<CopyCell, c_int> {
        let mut sc = tree.write_by_id(&sid).await?;
        for name in rc.children.iter() {
            if !sc.children.contains_key(name) {
                tree.create4parent(&mut sc, name).await?;
            }
        }

        let mut handles = Vec::new();
        let mut names = VecDeque::new();
        for (name, sid) in sc.children.iter() {
            let sid = *sid;
            let tree = tree.clone();
            let stge = stge.clone();
            let path = rc.path.join(name);
            let oneway = rc.oneway.clone();
            handles.push(tokio::spawn(async move {
                Self::make(sid, RemoteCell::from_ow(path, oneway).await, tree, stge).await
            }));
            names.push_back(name);
        }

        let mut children = Vec::new();
        for handle in join_all(handles).await {
            children.push((
                names.pop_front().unwrap().clone(),
                handle.map_err(|_| libc::EIO)??,
            ));
        }

        // todo!("sumup");

        Ok(Self::from_rc(
            stge.alloc_cid(),
            sid,
            rc,
            SyncOp::Recurse,
            sc.modif.clone(),
            children,
            stge,
        )
        .await)
    }

    pub async fn read(&self) -> impl AsyncSeekExt + AsyncReadExt + Unpin {
        self.stge.read_as_stream(&self.cid).await
    }
}

impl LeanCelled for CopyCell {
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

impl SyncCelled for CopyCell {
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

impl Display for CopyCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}(path: {:?}, modif: {:?}, sync: {:?}, crt: {})",
            self.ty, self.path, self.modif, self.sync, self.crt,
        )
    }
}
