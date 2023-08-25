use std::{
    cmp::max,
    collections::HashMap,
    fmt::Display,
    ops::{Deref, DerefMut},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};

use crate::{
    buffer::guard::{BufferReadGuard, BufferWriteGuard},
    disk::serde::PrefixSerdeDiskManager,
    fuse::meta::FileTy,
    rsync::hashed::{Hashed, HashedDelta, HashedList},
};

use super::{copy::SyncOp, lean::LeanCelled, time::VecTime, tree::SyncTree};

#[derive(Default, Deserialize, Serialize)]
pub struct SyncCell {
    /// And unique identifier to fetch `SyncCell` from disk.
    pub(crate) sid: u64,

    /// The parent of the `SyncCell`, which is unchanged after creation.
    pub(crate) parent: u64,

    /// The path of the file represented by `SyncCell`.
    pub(crate) path: PathBuf,

    /// The modification time vector.
    pub(crate) modif: VecTime,

    /// The synchronization time vector.
    pub(crate) sync: VecTime,

    /// The creating time, which is the minimum value in the modification history.
    pub(crate) crt: usize,

    /// Indicate the type.
    pub(crate) ty: FileTy,

    /// The synchronization would only recurse down when it has children.
    /// An empty directory could be regarded as a file.
    /// No difference in synchronization.
    pub(crate) children: HashMap<String, u64>,

    pub(crate) list: HashedList,
}

pub trait SyncCelled: LeanCelled {
    fn crt(&self) -> usize;
    fn ty(&self) -> FileTy;
    fn list(&self) -> &HashedList;
}

pub type SyncCellDiskManager = PrefixSerdeDiskManager<u64, SyncCell>;

pub struct SyncCellReadGuard<'a, const S: usize> {
    guard: BufferReadGuard<'a, u64, SyncCell, SyncCellDiskManager, S>,
    tree: SyncTree<S>,
}

pub struct SyncCellWriteGuard<'a, const S: usize> {
    guard: BufferWriteGuard<'a, u64, SyncCell, SyncCellDiskManager, S>,
    tree: SyncTree<S>,
}

impl SyncCell {
    /// Give `sync` beforehand to support inheritance.
    pub fn init(&mut self, sid: u64, parent: u64, path: PathBuf, sync: VecTime) {
        self.sid = sid;
        self.parent = parent;
        self.path = path;
        self.sync = sync;
    }

    pub fn empty(&mut self, sid: u64, parent: u64, path: PathBuf, ty: FileTy) {
        self.sid = sid;
        self.parent = parent;
        self.path = path;
        self.ty = ty;
    }

    pub fn create(&mut self, mid: usize, time: usize, ty: FileTy) {
        self.crt = time;
        self.ty = ty;
        self.update(mid, time);
        self.list.clear();
    }

    pub fn modify(&mut self, mid: usize, time: usize, delta: HashedDelta) {
        match delta {
            HashedDelta::Modify(begin, changes) => {
                if !changes.is_empty() {
                    let newlen = begin + changes.len();
                    if newlen > self.list.len() {
                        self.list.resize(newlen, Hashed::default());
                    }
                    for (idx, change) in changes.into_iter().enumerate() {
                        self.list[begin + idx] = change;
                    }
                }
            }

            HashedDelta::Shrink(len) => self.list.shrink_to(len),
        }
        self.update(mid, time);
    }

    pub fn remove(&mut self, mid: usize, time: usize) {
        self.ty = FileTy::None;
        self.update(mid, time);
        self.list.clear();
    }

    fn update(&mut self, mid: usize, time: usize) {
        if let Some(old_time) = self.modif.get(mid) {
            if old_time < time {
                self.modif.insert(mid, time);
                self.sync.insert(mid, time);
            }
        } else {
            self.modif.insert(mid, time);
            self.sync.insert(mid, time);
        }
    }

    pub fn merge(&mut self, other: &impl SyncCelled) {
        self.modif = other.modif().clone();
        self.sync.union(other.sync(), max);
    }

    pub fn substituted(&mut self, other: &impl SyncCelled) {
        self.merge(other);
        self.crt = other.crt();
        self.ty = other.ty();
        self.list = other.list().clone();
    }

    /// Synchronize the file with another server.
    pub fn calc_sync_op(&self, other: &impl SyncCelled) -> SyncOp {
        match (self.ty, other.ty()) {
            (FileTy::Dir, FileTy::Dir) => self.calc_sync_dir_op(other),
            _ => self.calc_sync_file_op(other),
        }
    }

    /// Do the sync job when both the src and dst are dirs.
    fn calc_sync_dir_op(&self, other: &impl SyncCelled) -> SyncOp {
        if self.ty != FileTy::None || other.ty() != FileTy::None {
            if other.modif() <= &self.sync {
                SyncOp::None
            } else {
                SyncOp::Recurse
            }
        } else {
            SyncOp::None
        }
    }

    /// Do the sync job when at least one of the src or the dst is not dir.
    fn calc_sync_file_op(&self, other: &impl SyncCelled) -> SyncOp {
        if self.ty == FileTy::None && other.ty() != FileTy::None {
            if other.modif() <= &self.sync {
                SyncOp::None
            } else if !(other.crt() <= self.sync) {
                if other.ty() == FileTy::Dir {
                    SyncOp::Recurse
                } else {
                    SyncOp::Copy
                }
            } else {
                SyncOp::Conflict
            }
        } else if self.ty != FileTy::None || other.ty() != FileTy::None {
            if other.modif() <= &self.sync {
                SyncOp::None
            } else if &self.modif <= other.sync() {
                if other.ty() == FileTy::Dir {
                    SyncOp::Recurse
                } else {
                    SyncOp::Copy
                }
            } else {
                SyncOp::Conflict
            }
        } else {
            SyncOp::None
        }
    }
}

impl LeanCelled for SyncCell {
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

impl SyncCelled for SyncCell {
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

impl<'a, const S: usize> SyncCellReadGuard<'a, S> {
    pub fn new(
        guard: BufferReadGuard<'a, u64, SyncCell, SyncCellDiskManager, S>,
        tree: SyncTree<S>,
    ) -> Self {
        Self { guard, tree }
    }

    pub async fn upgrade(self) -> SyncCellWriteGuard<'a, S> {
        SyncCellWriteGuard::new(self.guard.upgrade().await, self.tree)
    }
}

impl<'a, const S: usize> Deref for SyncCellReadGuard<'a, S> {
    type Target = SyncCell;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<'a, const S: usize> SyncCellWriteGuard<'a, S> {
    pub fn new(
        guard: BufferWriteGuard<'a, u64, SyncCell, SyncCellDiskManager, S>,
        tree: SyncTree<S>,
    ) -> Self {
        Self { guard, tree }
    }
}

impl<'a, const S: usize> Deref for SyncCellWriteGuard<'a, S> {
    type Target = SyncCell;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<'a, const S: usize> DerefMut for SyncCellWriteGuard<'a, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl Display for SyncCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}(path: {:?}, modif: {:?}, sync: {:?}, crt: {})",
            self.ty, self.path, self.modif, self.sync, self.crt,
        )
    }
}
