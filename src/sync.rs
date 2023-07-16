use std::{collections::HashMap, fmt::Debug, net::SocketAddr, ops::Sub, sync::Arc};

use async_recursion::async_recursion;
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use tokio::{fs, sync::Mutex};
use tracing::{error, info, instrument};

use crate::{
    comm::{Comm, Request, Response},
    copy::CopyCell,
    path::{AbsPath, RelPath},
    remote::RemoteCell,
    server::Server,
    time::VecTime,
};

#[derive(PartialEq, Eq, Deserialize, Serialize, Clone, Copy, Debug)]
pub enum CellType {
    File,
    Dir,
    None,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SyncOp {
    Copy,
    Conflict,
    Recurse,
    None,
}

/// The representation of synchronized file/dir in memory.
///
/// It's meaningless to distinguish whether it's a file or it's a dir.
/// Because the only important thing is synchronization and the only difference
/// in synchronization for file or dir is that dirs need to be recurse down
/// compared to files.
///
/// Therefore, the design of `SyncCell` is adopted.
/// `children` is only used for determining whether to recursing down the tree.
pub struct SyncCell {
    /// The path of the file, relative to the root sync dir.
    pub(super) rel: RelPath,

    pub(super) server: Arc<Server>,

    /// The inner data of `SyncCell`, which is a collection of data that might be modified.
    inner: Mutex<SyncCellInner>,
}

/// The inner data of `SyncCell`.
pub struct SyncCellInner {
    /// The modification time vector.
    pub(super) modif: VecTime,

    /// The synchronization time vector.
    pub(super) sync: VecTime,

    /// The creating time, which is the minimum value in the modification history.
    pub(super) crt: usize,

    /// The latest modification time of the current server.
    pub(super) ts: usize,

    /// Indicate the type.
    pub(super) ty: CellType,

    /// The synchronization would only recurse down when it has children.
    /// An empty directory could be regarded as a file.
    /// No difference in synchronization.
    pub(super) children: HashMap<RelPath, Arc<SyncCell>>,
}

// helper functions
impl SyncCell {
    pub fn try_lock(
        &self,
    ) -> Result<tokio::sync::MutexGuard<'_, SyncCellInner>, tokio::sync::TryLockError> {
        self.inner.try_lock()
    }

    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, SyncCellInner> {
        self.inner.lock().await
    }

    pub fn path(&self) -> AbsPath {
        &self.server.root.clone() + &self.rel
    }

    /// Send up the metadata through the path to the root directory.
    pub async fn sendup_meta(self: Arc<SyncCell>) {
        let (modif, sync) = {
            let self_guard = self.lock().await;
            (self_guard.modif.clone(), self_guard.sync.clone())
        };
        let mut rel = RelPath::default();
        let mut curr = self.server.clone().make_sc(&RelPath::default()).await;
        info!("{:?}", curr);
        let delta_path = self.rel.as_delta_path_buf();
        let comps: Vec<_> = delta_path.components().collect();
        for comp in comps {
            let mut curr_guard = curr.lock().await;
            curr_guard.modif.merge_max(&modif);
            curr_guard.sync.merge_min(&sync);
            let comp = RelPath::from(comp.as_os_str());
            rel += &comp;
            info!("{:?} {:?}", rel, curr_guard.children);
            let next = curr_guard.children.get(&rel).unwrap().clone();
            drop(curr_guard);
            curr = next;
        }
        // let mut last = self.clone();
        // while let Some(curr) = last.parent.as_ref().map(|c| c.upgrade().unwrap()) {
        //     let mut curr_guard = curr.lock().await;
        //     curr_guard.modif.merge_max(&modif);
        //     curr_guard.sync.merge_min(&sync);
        //     last = curr.clone()
        // }
    }
}

// conversion utilities
impl SyncCell {
    /// Convert the `SyncCell` to the `RemoteCell` for server to use.
    pub async fn into_rc(&self, listener: SocketAddr) -> RemoteCell {
        let self_guard = self.lock().await;
        let mut children = Vec::new();
        for (path, child) in self_guard.children.iter() {
            children.push((path.clone(), child.lock().await.ty));
        }
        RemoteCell::new(
            self.rel.clone(),
            self_guard.modif.clone(),
            self_guard.sync.clone(),
            self_guard.crt.clone(),
            self_guard.ty.clone(),
            children,
            listener,
        )
    }
}

// constructor
impl SyncCell {
    pub fn new(
        server: &Arc<Server>,
        path: &RelPath,
        ty: CellType,
        modif: VecTime,
        sync: VecTime,
        crt: usize,
        cur: usize,
        children: HashMap<RelPath, Arc<SyncCell>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            rel: path.clone(),
            server: server.clone(),
            inner: Mutex::new(SyncCellInner {
                modif,
                sync,
                crt,
                ts: cur,
                ty,
                children,
            }),
        })
    }

    /// The `empty` function doesn't maintain the file tree due to the latent risk for dead lock.
    pub async fn empty(server: &Arc<Server>, path: &RelPath) -> Arc<Self> {
        let cell = Self::new(
            server,
            path,
            CellType::None,
            VecTime::new(),
            VecTime::new(),
            0,
            0,
            HashMap::new(),
        );

        // add to the server
        server.add_sc(&cell).await;

        cell
    }
}

impl SyncCellInner {
    /// Sum up the metadata information from all of its children.
    pub async fn sum_children(&mut self) {
        let mut modif = self.modif.clone();
        let mut sync = self.sync.clone();
        for (_, child) in self.children.iter() {
            let child_guard = child.lock().await;
            modif.merge_max(&child_guard.modif);
            sync.merge_min(&child_guard.sync);
        }

        self.modif = modif;
        self.sync = sync;
    }

    /// Synchronize the meta data with the remote server.
    ///
    /// The metadata indicates the modification time vector and synchronization time vector.
    pub fn sync_meta(&mut self, other: &RemoteCell) {
        self.modif.merge_max(&other.modif);
        self.sync.merge_min(&other.sync);
    }
}

impl SyncCellInner {
    /// Check whether a file is existing in the file system.
    /// Use `metadata` provided by `tokio` to check.
    ///
    /// Better to be called with `tokio::task::spawn_blocking`.
    ///
    /// However, in the current implementation, the metadata in the fs is not check.
    /// Instead, we just check the `CellType`.
    pub fn is_existing(&self) -> bool {
        self.ty != CellType::None
    }

    pub fn get_child(&self, path: &RelPath) -> Option<Arc<SyncCell>> {
        self.children.get(path).map(|c| c.clone())
    }

    pub fn add_child(&mut self, cell: Arc<SyncCell>) {
        self.children.insert(cell.rel.clone(), cell);
    }

    pub fn has_child(&self, path: &RelPath) -> bool {
        self.children.contains_key(path)
    }
}

impl Debug for SyncCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let self_guard = self.try_lock();

        let mut d = f.debug_struct("TraCell");
        d.field("rel", &self.rel);

        if let Ok(self_guard) = self_guard {
            d.field("modif", &self_guard.modif)
                .field("sync", &self_guard.sync)
                .field("ty", &self_guard.ty);
        } else {
            d.field("modif", &"locked")
                .field("sync", &"locked")
                .field("ty", &"locked");
        }

        d.finish()
    }
}

impl Debug for SyncCellInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraCellInner")
            .field("modif", &self.modif)
            .field("sync", &self.sync)
            .field("ty", &self.ty)
            .finish()
    }
}

// sync part

impl SyncCell {
    #[instrument]
    pub async fn sync(self: Arc<Self>, addr: SocketAddr) {
        info!("begin to sync");
        let sid = self
            .server
            .sid
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut path = self.server.tmp_path().as_path_buf();
        path.push(sid.to_string());
        fs::create_dir_all(path).await.unwrap();
        let rc = RemoteCell::from_path(addr, self.rel.clone()).await;
        let cc = CopyCell::make(sid, self.clone(), rc, self.rel.parent()).await;
        info!("copy done");
        self.clone().sync_copy(sid, cc, addr).await;
        info!("sync done");

        // begin to remove cache
        let path = self.server.tmp_path().as_path_buf().join(sid.to_string());
        let meta = fs::metadata(&path).await;
        if let Ok(meta) = meta {
            if meta.is_dir() {
                fs::remove_dir_all(&path).await.unwrap();
            } else {
                fs::remove_file(&path).await.unwrap();
            }
        }
    }

    #[instrument]
    #[async_recursion]
    pub async fn sync_copy(self: Arc<Self>, sid: usize, cc: CopyCell, addr: SocketAddr) {
        if cc.op != SyncOp::None {
            let mut self_guard = self.lock().await;
            if self_guard.ts == cc.ts {
                match cc.op {
                    SyncOp::Copy => {
                        info!("rename {:?} -> {:?}", cc.path(), self.path());
                        if let Err(e) = fs::rename(cc.path(), self.path().as_path_buf()).await {
                            error!("{:?} with {:?} and {:?}", e, cc.path(), self.path());
                        }
                        self_guard.reshape_from_cc(&cc);
                    }

                    SyncOp::Conflict => {
                        let mut bak = self.path().as_path_buf();
                        let fname = bak.file_name();
                        if let Some(fname) = fname {
                            // modify the filename
                            let mut fname = String::from(fname.to_str().unwrap());
                            fname.push_str(".rfsync");
                            bak.set_file_name(fname);
                            info!("rename {:?} -> {:?}", cc.path(), bak);

                            // check whether the rename procedure is successful
                            if let Err(e) = fs::rename(cc.path(), &bak).await {
                                error!("{:?} with {:?}; and {:?}", e, cc.path(), bak);
                            }
                            self_guard.reshape_from_cc(&cc);
                        } else {
                            error!("{:?} is not a file", bak);
                        }
                    }

                    SyncOp::Recurse => {
                        info!("recurse");
                        let mut handles = Vec::new();
                        for cc in cc.children.iter() {
                            // create a sc if there is none
                            let sc = if self_guard.children.contains_key(&cc.offset) {
                                self_guard.children.get(&cc.offset).unwrap().clone()
                            } else {
                                let sc = Self::empty(&self.server, &cc.rel).await;
                                self_guard.children.insert(cc.offset.clone(), sc.clone());
                                sc
                            };
                            handles.push(tokio::spawn(sc.sync_copy(sid, cc.clone(), addr)));
                        }
                        join_all(handles).await;
                    }

                    SyncOp::None => {
                        info!("none");
                        // do nothing
                    }
                }
            } else {
                info!("redo");
                // we need to redo the sync
                let rc = RemoteCell::from_path(addr, self.rel.clone()).await;
                let cc = CopyCell::make(sid, self.clone(), rc, self.rel.clone()).await;
                self.clone().sync_copy(sid, cc, addr).await;
            }
        }
    }
}

impl SyncCellInner {
    pub fn reshape_from_cc(&mut self, cc: &CopyCell) {
        self.modif = cc.modif.clone();
        self.sync = cc.sync.clone();
        self.crt = cc.crt;
        self.ty = cc.ty;
    }
}

impl SyncCellInner {
    /// Synchronize the file with another server.
    #[instrument]
    pub(super) fn get_sop(&self, rc: &RemoteCell) -> SyncOp {
        match (self.ty, rc.ty) {
            (CellType::Dir, CellType::Dir) => self.get_sdop(rc),
            _ => self.get_sfop(rc),
        }
    }

    /// Do the sync job when both the src and dst are dirs.
    #[instrument]
    fn get_sdop(&self, rc: &RemoteCell) -> SyncOp {
        if self.is_existing() || rc.is_existing() {
            if rc.modif <= self.sync {
                SyncOp::None
            } else {
                SyncOp::Recurse
            }
        } else {
            SyncOp::None
        }
    }

    /// Do the sync job when at least one of the src or the dst is not dir.
    #[instrument]
    fn get_sfop(&self, rc: &RemoteCell) -> SyncOp {
        if !self.is_existing() && rc.is_existing() {
            if rc.modif <= self.sync {
                SyncOp::None
            } else if !(rc.crt <= self.sync) {
                SyncOp::Copy
            } else {
                SyncOp::Conflict
            }
        } else if self.is_existing() || rc.is_existing() {
            if rc.modif <= self.sync {
                SyncOp::None
            } else if self.modif <= rc.sync {
                SyncOp::Copy
            } else {
                SyncOp::Conflict
            }
        } else {
            SyncOp::None
        }
    }
}

// modification part

impl SyncCell {
    /// Create a new file or directory. Increase the server time while creating.
    ///
    /// If it's a directory, it would recurse down, adding all of its component to the server.
    #[instrument]
    #[async_recursion]
    pub async fn create(self: Arc<Self>, time: usize) {
        let mut self_guard = self.lock().await;
        let path = self.path().as_path_buf();

        // update modif vectime
        if !self_guard.modify(&self.server, time) {
            return;
        }
        if let Ok(metadata) = fs::metadata(&path).await {
            if metadata.is_dir() {
                if self_guard.ty != CellType::Dir {
                    self_guard.ty = CellType::Dir;
                }

                // recurse down
                let dir = fs::read_dir(&path).await;
                let mut handles = Vec::new();
                if let Ok(mut stream) = dir {
                    while let Some(entry) = stream.next_entry().await.unwrap() {
                        let path = AbsPath::new(entry.path()).sub(&self.server.root).unwrap();

                        // ignore all bak files
                        if let Some(ext) = path.extension() && ext == "rfsync" {
                            continue;
                        }

                        let child = if let Some(child) = self_guard.get_child(&path) {
                            child
                        } else {
                            let cell = Self::empty(&self.server, &path).await;
                            self_guard.add_child(cell.clone());

                            cell
                        };
                        handles.push(tokio::spawn(child.create(time)));
                    }
                } else {
                    // maybe some consistency conflicts
                }

                // wait for all children to finish
                info!("create {:?}", self_guard);
                drop(self_guard);
                join_all(handles).await;
                self.lock().await.sum_children().await;
            } else {
                // file
                self_guard.ty = CellType::File;
                info!("create {:?}", self_guard);
                drop(self_guard);
            }
        } else {
            drop(self_guard);
            // there might be some other file system changes
        }
    }

    #[instrument]
    #[async_recursion]
    pub async fn remove(self: Arc<Self>, time: usize) {
        let handles = {
            let mut self_guard = self.lock().await;

            // update modif vectime
            if !self_guard.modify(&self.server, time) {
                return;
            }

            self_guard.ty = CellType::None;
            let mut handles = Vec::new();
            for child in self_guard.children.values() {
                let child = child.clone();
                if child.lock().await.ty == CellType::Dir {
                    handles.push(tokio::spawn(child.remove(time)));
                }
            }
            handles
        };
        join_all(handles).await;
        self.lock().await.sum_children().await;
    }

    #[instrument]
    pub async fn modify(self: Arc<Self>, time: usize) {
        let mut self_guard = self.lock().await;
        self_guard.modify(&self.server, time);
    }

    #[instrument]
    #[async_recursion]
    pub async fn broadcast(self: Arc<Self>) {
        info!("broadcast");
        let server = self.server.clone();
        for peer in server.peers.read().await.iter() {
            if peer.id == server.id {
                continue;
            }

            let req = Request::SyncCell(server.as_ref().into(), self.rel.clone());
            info!("send {:?}", req);
            let res = Comm::new(peer.addr).request(&req).await;

            match res {
                Response::Sync => {
                    // do nothing
                }
                _ => panic!("unexpected response"),
            }
        }
    }
}

impl SyncCellInner {
    pub fn modify(&mut self, server: &Arc<Server>, time: usize) -> bool {
        let id = server.id;
        if self.ts <= time {
            if self.modif.is_empty() {
                self.crt = time;
            }
            self.modif.insert(id, time);
            self.ts = time;
            true
        } else {
            false
        }
    }
}
