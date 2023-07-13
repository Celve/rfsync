pub mod copy;
pub mod modif;
pub mod remote;
pub mod sync;

use std::{
    collections::HashMap,
    fmt::Debug,
    net::SocketAddr,
    sync::{Arc, Weak},
};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::info;

use crate::{
    path::{AbsPath, RelPath},
    server::Server,
    time::VecTime,
};

use self::remote::RemoteCell;

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
pub struct TraCell {
    /// The path of the file, relative to the root sync dir.
    pub(super) rel: RelPath,

    pub(super) parent: Option<Weak<TraCell>>,

    pub(super) server: Arc<Server>,

    /// The inner data of `SyncCell`, which is a collection of data that might be modified.
    inner: Mutex<TraCellInner>,
}

/// The inner data of `SyncCell`.
pub struct TraCellInner {
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
    pub(super) children: HashMap<RelPath, Arc<TraCell>>,
}

// helper functions
impl TraCell {
    pub fn try_lock(
        &self,
    ) -> Result<tokio::sync::MutexGuard<'_, TraCellInner>, tokio::sync::TryLockError> {
        self.inner.try_lock()
    }

    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, TraCellInner> {
        self.inner.lock().await
    }

    pub fn path(&self) -> AbsPath {
        &self.server.root.clone() + &self.rel
    }

    /// Send up the metadata through the path to the root directory.
    pub async fn sendup_meta(self: Arc<TraCell>) {
        let (modif, sync) = {
            let self_guard = self.lock().await;
            (self_guard.modif.clone(), self_guard.sync.clone())
        };
        let mut rel = RelPath::default();
        let mut curr = self.server.clone().make_tc(&RelPath::default()).await;
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
impl TraCell {
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
impl TraCell {
    pub fn new(
        server: &Arc<Server>,
        path: &RelPath,
        parent: Option<Weak<TraCell>>,
        ty: CellType,
        modif: VecTime,
        sync: VecTime,
        crt: usize,
        cur: usize,
        children: HashMap<RelPath, Arc<TraCell>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            rel: path.clone(),
            parent,
            server: server.clone(),
            inner: Mutex::new(TraCellInner {
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
    pub async fn empty(
        server: &Arc<Server>,
        path: &RelPath,
        parent: Option<Weak<TraCell>>,
    ) -> Arc<Self> {
        let cell = Self::new(
            server,
            path,
            parent,
            CellType::None,
            VecTime::new(),
            VecTime::new(),
            0,
            0,
            HashMap::new(),
        );

        // add to the server
        server.add_tc(&cell).await;

        cell
    }
}

impl TraCellInner {
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
    pub async fn sync_meta(&mut self, other: &RemoteCell) {
        self.modif.merge_max(&other.modif);
        self.sync.merge_min(&other.sync);
    }
}

impl TraCellInner {
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

    pub fn get_child(&self, path: &RelPath) -> Option<Arc<TraCell>> {
        self.children.get(path).map(|c| c.clone())
    }

    pub fn add_child(&mut self, cell: Arc<TraCell>) {
        self.children.insert(cell.rel.clone(), cell);
    }

    pub fn has_child(&self, path: &RelPath) -> bool {
        self.children.contains_key(path)
    }
}

impl Debug for TraCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let self_guard = self.try_lock();

        let mut d = f.debug_struct("SyncCell");
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

impl Debug for TraCellInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraCellInner")
            .field("modif", &self.modif)
            .field("sync", &self.sync)
            .field("ty", &self.ty)
            .finish()
    }
}
