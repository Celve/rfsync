use std::{fmt::Debug, ops::Sub, path::PathBuf, sync::Arc};

use async_recursion::async_recursion;
use futures_util::future::join_all;
use tokio::fs;
use tracing::{error, info, instrument};

use crate::{path::RelPath, server::Server, time::VecTime};

use super::{
    remote::RemoteCell,
    sync::{CellType, SyncCell, SyncOp},
};

#[derive(Clone)]
pub struct CopyCell {
    pub(super) sid: usize,

    /// The path of the root of the copy job.
    pub(super) offset: RelPath,

    /// The path of the file, relative to the root sync dir.
    pub(super) rel: RelPath,

    pub(super) sc: Arc<SyncCell>,

    /// The modification time vector.
    pub(super) modif: VecTime,

    /// The synchronization time vector.
    pub(super) sync: VecTime,

    /// The creationg time, which is the minimum value in the modification history.
    pub(super) crt: usize,

    /// Indicate the type.
    pub(super) ty: CellType,

    /// Indicate the sync operation.
    pub(super) op: SyncOp,

    /// The timestamp at the beginning of the sync.
    pub(super) ts: usize,

    /// Only use `PathBuf` because its children hasn't been fetched from remote.
    pub(super) children: Vec<CopyCell>,
}

impl CopyCell {
    /// Generate `CopyCell` tree for further synchronization.
    #[instrument]
    #[async_recursion]
    pub async fn make(sid: usize, sc: Arc<SyncCell>, rc: RemoteCell, offset: RelPath) -> Self {
        let sc_guard = sc.lock().await;
        let sync_op = sc_guard.get_sop(&rc);
        let ts = sc_guard.ts;
        drop(sc_guard);

        info!("copy {:?}", sync_op);

        match sync_op {
            SyncOp::Copy => Self::copy_from_rc(sid, offset.clone(), sc.clone(), &rc, ts).await,
            SyncOp::Conflict => Self::conflict(sid, offset.clone(), sc.clone(), &rc, ts).await,
            SyncOp::Recurse => Self::recurse(sid, offset.clone(), sc.clone(), &rc, ts).await,
            SyncOp::None => Self::none(sid, offset.clone(), sc.clone(), &rc, ts).await,
        }
    }

    /// Create a `TmpCell` that represents that there is nothing to do for sync.
    pub async fn none(
        sid: usize,
        offset: RelPath,
        sc: Arc<SyncCell>,
        rc: &RemoteCell,
        ts: usize,
    ) -> Self {
        Self::new(
            sid,
            offset,
            rc.rel.clone(),
            sc,
            VecTime::new(),
            VecTime::new(),
            0,
            CellType::None, // meaningless
            SyncOp::None,
            ts,
            Vec::new(),
        )
        .await
    }

    /// Report a conflict in synchronization.
    ///
    /// Because it's a conflict, we should create a replica in the `/tmp` directory.
    pub async fn conflict(
        sid: usize,
        offset: RelPath,
        sc: Arc<SyncCell>,
        rc: &RemoteCell,
        ts: usize,
    ) -> Self {
        let path = Self::path_from_raw(sid, &sc.server, &rc.rel, &offset);

        let content = rc.read_file().await;
        fs::write(path, content).await.unwrap();

        Self::new(
            sid,
            offset,
            rc.rel.clone(),
            sc.clone(),
            rc.modif.clone(),
            rc.sync.clone(),
            0,
            rc.ty, // it should be file
            SyncOp::Conflict,
            ts,
            Vec::new(),
        )
        .await
    }

    #[instrument]
    #[async_recursion]
    pub async fn copy_from_rc(
        sid: usize,
        offset: RelPath,
        sc: Arc<SyncCell>,
        rc: &RemoteCell,
        ts: usize,
    ) -> Self {
        let path = Self::path_from_raw(sid, &sc.server, &rc.rel, &offset);

        let children = if rc.ty == CellType::Dir {
            // begin to watch
            if let Err(e) = fs::create_dir_all(&path).await {
                error!("{:?}", e);
            }

            let mut handles = Vec::new();
            for (path, _) in rc.children.iter() {
                let sc = {
                    let mut sc_guard = sc.lock().await;
                    if sc_guard.has_child(path) {
                        sc_guard.get_child(path).unwrap()
                    } else {
                        let sc = SyncCell::empty(&sc.server, path).await;
                        sc_guard.add_child(sc.clone());
                        sc
                    }
                };
                let path = path.clone();
                let addr = rc.addr;
                let offset = offset.clone();
                handles.push(tokio::spawn(async move {
                    let rc = RemoteCell::from_path(addr, path.clone()).await;
                    Self::copy_from_rc(sid, offset.clone(), sc, &rc, ts).await
                }));
            }

            info!("recursively copy {:?}", path);

            let children: Vec<CopyCell> = join_all(handles)
                .await
                .iter()
                .map(|r| r.as_ref().unwrap().clone())
                .collect();
            children
        } else {
            let content = rc.read_file().await;
            if let Err(e) = fs::write(&path, content).await {
                error!("{:?} with {:?}", e, path);
            }
            info!("copy to file {:?}", path);
            Vec::new()
        };
        let cc = Self::new(
            sid,
            offset,
            rc.rel.clone(),
            sc.clone(),
            rc.modif.clone(),
            rc.sync.clone(),
            rc.crt,
            rc.ty,
            SyncOp::Copy,
            ts,
            children,
        )
        .await;

        cc
    }

    #[instrument]
    pub async fn recurse(
        sid: usize,
        offset: RelPath,
        sc: Arc<SyncCell>,
        rc: &RemoteCell,
        ts: usize,
    ) -> Self {
        let path = Self::path_from_raw(sid, &sc.server, &rc.rel, &offset);
        if let Err(e) = fs::create_dir_all(&path).await {
            error!("{:?}", e);
        }
        info!("create {:?}", path);

        // make the cc
        let mut cc = Self::new(
            sid,
            offset.clone(),
            rc.rel.clone(),
            sc.clone(),
            rc.modif.clone(),
            rc.sync.clone(),
            rc.crt,
            rc.ty,
            SyncOp::Recurse,
            ts,
            Vec::new(),
        )
        .await;
        let mut handles = Vec::new();
        let mut sc_guard = sc.lock().await;
        for (path, child) in sc_guard.children.iter() {
            if child.lock().await.is_existing() {
                let path = path.clone();
                let addr = rc.addr.clone();
                let offset = offset.clone();
                handles.push(tokio::spawn(Self::make(
                    sid,
                    child.clone(),
                    RemoteCell::from_path(addr, path).await,
                    offset,
                )));
            }
        }
        for (path, _) in rc.children.iter() {
            if !sc_guard.children.contains_key(path) {
                let sc = SyncCell::empty(&sc.server, path).await;
                sc_guard.add_child(sc.clone());
                let path = path.clone();
                let addr = rc.addr.clone();
                let offset = offset.clone();
                handles.push(tokio::spawn(Self::make(
                    sid,
                    sc,
                    RemoteCell::from_path(addr, path).await,
                    offset,
                )));
            }
        }
        let children: Vec<CopyCell> = join_all(handles)
            .await
            .iter()
            .map(|r| r.as_ref().unwrap().clone())
            .collect();
        cc.children = children;

        cc
    }
}

// utilities
impl CopyCell {
    async fn new(
        sid: usize,
        offset: RelPath,
        rel: RelPath,
        sc: Arc<SyncCell>,
        modif: VecTime,
        sync: VecTime,
        crt: usize,
        ty: CellType,
        op: SyncOp,
        ts: usize,
        children: Vec<CopyCell>,
    ) -> Self {
        Self {
            sid,
            offset,
            rel,
            sc,
            modif,
            sync,
            crt,
            ty,
            op,
            ts,
            children,
        }
    }

    pub fn path(&self) -> PathBuf {
        Self::path_from_raw(self.sid, &self.sc.server, &self.rel, &self.offset)
    }

    pub fn path_from_raw(
        sid: usize,
        server: &Arc<Server>,
        rel: &RelPath,
        offset: &RelPath,
    ) -> PathBuf {
        let mut path = PathBuf::from(server.tmp_path());
        path.push(sid.to_string());
        path.push(rel.sub(&offset).unwrap().as_delta_path_buf());
        path
    }
}

impl Debug for CopyCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CopyCell")
            .field("sid", &self.sid)
            .field("offset", &self.offset)
            .field("rel", &self.rel)
            .field("modif", &self.modif)
            .field("sync", &self.sync)
            .field("crt", &self.crt)
            .field("ty", &self.ty)
            .field("op", &self.op)
            .field("ts", &self.ts)
            .finish()
    }
}
