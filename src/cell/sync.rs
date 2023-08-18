use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};

use crate::{
    buffer::guard::{BufferReadGuard, BufferWriteGuard},
    fuse::{disk::PrefixDiskManager, meta::FileTy},
};

use super::{lean::LeanCell, time::VecTime, tree::SyncTree};

#[derive(Default, Deserialize, Serialize)]
pub struct SyncCell {
    /// And unique identifier to fetch `SyncCell` from disk.
    pub(super) sid: u64,

    /// The path of the file represented by `SyncCell`.
    pub path: PathBuf,

    /// The modification time vector.
    pub modif: VecTime,

    /// The synchronization time vector.
    pub(super) sync: VecTime,

    /// The creating time, which is the minimum value in the modification history.
    pub(super) crt: usize,

    /// Indicate the type.
    pub(super) ty: FileTy,

    /// The synchronization would only recurse down when it has children.
    /// An empty directory could be regarded as a file.
    /// No difference in synchronization.
    pub(super) children: HashMap<String, u64>,
}

pub type SyncCellDiskManager = PrefixDiskManager<u64, SyncCell>;

pub struct SyncCellReadGuard<'a, const S: usize> {
    guard: BufferReadGuard<'a, u64, SyncCell, SyncCellDiskManager, S>,
    tree: SyncTree<S>,
}

pub struct SyncCellWriteGuard<'a, const S: usize> {
    guard: BufferWriteGuard<'a, u64, SyncCell, SyncCellDiskManager, S>,
    tree: SyncTree<S>,
}

impl SyncCell {
    pub fn create(&mut self, sid: u64, path: PathBuf, ty: FileTy) {
        self.sid = sid;
        self.path = path;
        self.ty = ty;
    }

    pub fn modify(&mut self, mid: usize, time: usize, ty: FileTy) {
        if self.ty == FileTy::None {
            self.crt = time;
        }
        self.ty = ty;

        if let Some(old_time) = self.modif.get(mid) {
            if old_time < time {
                self.modif.insert(mid, time);
            }
        } else {
            self.modif.insert(mid, time);
        }
    }

    pub fn update(&mut self, mid: usize, time: usize) {
        self.sync.insert(mid, time);
    }
}

impl<'a, const S: usize> SyncCellReadGuard<'a, S> {
    pub fn new(
        guard: BufferReadGuard<'a, u64, SyncCell, SyncCellDiskManager, S>,
        tree: SyncTree<S>,
    ) -> Self {
        Self { guard, tree }
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

impl<'a, const S: usize> Drop for SyncCellWriteGuard<'a, S> {
    fn drop(&mut self) {
        let srv = self.tree.clone();
        let lc = LeanCell::from((*self).deref());
        tokio::spawn(async move { srv.sendup(lc).await });
    }
}
