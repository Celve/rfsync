use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    net::SocketAddr,
    sync::{Arc, Weak},
};

use async_recursion::async_recursion;
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, metadata, File},
    io::AsyncWriteExt,
    sync::Mutex,
};
use tracing::instrument;

use crate::{
    path::RelativePath,
    remote::RemoteCell,
    server::Server,
    time::VecTime,
    watcher::{FileEventType, Watcher},
};

#[derive(PartialEq, Eq, Deserialize, Serialize, Clone, Copy, Debug)]
pub enum CellType {
    File,
    Dir,
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
    pub(super) path: RelativePath,

    pub(super) parent: Option<Weak<SyncCell>>,

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

    /// The creationg time, which is the minimum value in the modification history.
    pub(super) crt: usize,

    /// Indicate the type.
    pub(super) ty: CellType,

    /// The synchronization would only recurse down when it has children.
    /// An empty directory could be regarded as a file.
    /// No difference in synchronization.
    pub(super) children: HashMap<RelativePath, Arc<SyncCell>>,
}

// sync functions
impl SyncCell {
    /// Synchronize the file with another server.
    #[async_recursion]
    pub async fn sync_cell(self: Arc<Self>, other: RemoteCell) {
        self.sync_meta(&other).await;
        match (self.lock().await.ty.clone(), other.ty) {
            (CellType::Dir, CellType::Dir) => self.clone().sync_dirs(other).await,
            _ => self.clone().sync_files(other).await,
        }
    }

    /// Do the sync job when both the src and dst are dirs.
    pub async fn sync_dirs(self: Arc<Self>, other: RemoteCell) {
        if self.is_existing().await || other.is_existing() {
            if other.modif <= self.lock().await.sync {
                // do nothing
            } else {
                self.clone().recurse_children(other).await;
            }
        }
        self.sum_children().await;
    }

    /// Do the sync job when at least one of the src or the dst is not dir.
    pub async fn sync_files(self: Arc<Self>, other: RemoteCell) {
        if !self.is_existing().await && other.is_existing() {
            let self_guard = self.lock().await;
            if other.modif <= self_guard.sync {
                // do nothing
            } else if !(other.crt <= self_guard.sync) {
                // copy from other to self
                self.clone().copy_from_rc(&other).await;
            } else {
                todo!("report a conflict")
            }
        } else {
            let self_guard = self.lock().await;
            if other.modif <= self_guard.sync {
                // do nothing
            } else if self_guard.modif <= other.sync {
                // copy from other to self
                self.clone().copy_from_rc(&other).await;
            } else {
                todo!("report a conflict")
            }
        }
    }
}

// copy from remote utilities
impl SyncCell {
    /// Copy the meta and file in fs from the remote server to the local server.
    ///
    /// Modify the current `SyncCell` at the same time.
    pub async fn copy_from_rc(self: Arc<Self>, other: &RemoteCell) {
        if other.is_existing() {
            // update meta
            let mut self_guard = self.lock().await;
            self_guard.ty = other.ty;
            self_guard.crt = other.crt;
            if other.ty == CellType::Dir {
                // self.children = other.children.clone();
                self_guard.children = HashMap::new();
                for (path, ty) in other.children.iter() {
                    // Self::from_parent(self.server.clone(), self.clone(), path, *ty).await;
                    self.server
                        .clone()
                        .make_sc_from_parent(self.clone(), path, *ty)
                        .await;
                }
            }

            // update file in file system
            let data = other.read_file().await;
            self.write_file(&data).await;
        } else {
            // update meta
            let mut self_guard = self.lock().await;
            self_guard.ty = CellType::None;
            self_guard.children.clear();

            // delete the file in file system
            self.remove_file().await;
        }
    }

    pub async fn merge_children(self: Arc<Self>, other: &RemoteCell) {
        // dirs in self
        let mut children = HashSet::new();
        for (path, _) in self.lock().await.children.iter() {
            children.insert(path.clone());
        }

        // dirs in other
        for (path, ty) in other.children.iter() {
            if !children.contains(path) {
                self.server
                    .clone()
                    .make_sc_from_parent(self.clone(), path, *ty)
                    .await;
                children.insert(path.clone());
            }
        }
    }

    /// Synchronize the directory recursively, which is the union of the local and the remote.
    pub async fn recurse_children(self: Arc<Self>, other: RemoteCell) {
        self.clone().merge_children(&other).await;
        let self_guard = self.lock().await;
        for (path, child) in self_guard.children.iter() {
            let child = child.clone();
            let path = path.clone();
            tokio::spawn(
                async move {
                    let other = RemoteCell::from_path(other.remote, path).await;
                    child.sync_cell(other).await;
                }
                .boxed(),
            );
        }
    }
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

    /// Check whether a file is existing in the file system.
    /// Use `metadata` provided by `tokio` to check.
    ///
    /// Better to be called with `tokio::task::spawn_blocking`.
    ///
    /// However, in the current implementation, the metadata in the fs is not check.
    /// Instead, we just check the `CellType`.
    pub async fn is_existing(&self) -> bool {
        // fs::metadata(&self.path).await.is_ok()
        self.lock().await.ty != CellType::None
    }

    /// Write content in buffer to the beginning of the given file.
    pub async fn write_file(&self, data: &Vec<u8>) {
        let mut f = File::create(self.path.as_path_buf()).await.unwrap();
        f.write(data).await.unwrap();
    }

    /// Delete the corresponding file in the file system
    pub async fn remove_file(&self) {
        fs::remove_file(self.path.as_path_buf()).await.unwrap();
    }

    /// Synchronize the meta data with the remote server.
    ///
    /// The metadata indicates the modification time vector and synchronization time vector.
    pub async fn sync_meta(&self, other: &RemoteCell) {
        let mut self_guard = self.lock().await;
        self_guard.modif.merge_max(&other.modif);
        self_guard.sync.merge_min(&other.sync);
    }

    pub async fn sendup_meta(self: Arc<SyncCell>) {
        let (modif, sync) = {
            let self_guard = self.lock().await;
            (self_guard.modif.clone(), self_guard.sync.clone())
        };
        let mut last = self.clone();
        while let Some(curr) = last.parent.as_ref().map(|c| c.upgrade().unwrap()) {
            let mut curr_guard = curr.lock().await;
            curr_guard.modif.merge_max(&modif);
            curr_guard.sync.merge_min(&sync);
            last = curr.clone()
        }
    }

    pub async fn sum_children(&self) {
        let mut self_guard = self.lock().await;
        let mut modif = self_guard.modif.clone();
        let mut sync = self_guard.sync.clone();
        for (_, child) in self_guard.children.iter() {
            let child_guard = child.lock().await;
            modif.merge_max(&child_guard.modif);
            sync.merge_min(&child_guard.sync);
        }

        self_guard.modif = modif;
        self_guard.sync = sync;
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
            self.path.clone(),
            self_guard.modif.clone(),
            self_guard.sync.clone(),
            self_guard.crt.clone(),
            self_guard.ty.clone(),
            children,
            listener,
        )
    }
}

// watcher
impl SyncCell {
    pub fn watch(self: Arc<Self>) {
        tokio::spawn(self.run_watcher());
    }

    /// Monitor the file system.
    #[instrument]
    async fn run_watcher(self: Arc<Self>) {
        let server = self.server.clone();

        // begin to watch
        let watcher = Watcher::new(server.path.clone(), self.path.clone());
        let mut rx = watcher.subscribe().await;
        watcher.clone().watch();

        while let Some(event) = rx.recv().await {
            let path = event.path;
            match event.ty {
                FileEventType::Create => {
                    let metadata = metadata(path.as_path_buf()).await;
                    if let Ok(metadata) = metadata {
                        let ty = if metadata.is_dir() {
                            CellType::Dir
                        } else {
                            CellType::File
                        };

                        server.clone().create(&path, ty).await;
                    }
                }
                FileEventType::Delete => {
                    server.clone().remove(&path).await;
                    server.get_sc(&path).await.unwrap();
                }
                FileEventType::Modify => {
                    server.clone().modify(&path).await;
                }
            }
        }
    }
}

// constructor
impl SyncCell {
    pub fn new(
        server: &Arc<Server>,
        path: &RelativePath,
        parent: Option<Weak<SyncCell>>,
        ty: CellType,
        modif: VecTime,
        sync: VecTime,
        crt: usize,
        children: HashMap<RelativePath, Arc<SyncCell>>,
    ) -> Arc<Self> {
        let cell = Arc::new(Self {
            path: path.clone(),
            parent,
            server: server.clone(),
            inner: Mutex::new(SyncCellInner {
                modif,
                sync,
                crt,
                ty,
                children,
            }),
        });

        cell
    }
}

impl Debug for SyncCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let self_guard = self.try_lock();

        let mut d = f.debug_struct("SyncCell");
        d.field("path", &self.path);

        if let Ok(self_guard) = self_guard {
            d.field("modif", &self_guard.modif)
                .field("sync", &self_guard.sync);
        } else {
            d.field("modif", &"locked").field("sync", &"locked");
        }

        d.finish()
    }
}
