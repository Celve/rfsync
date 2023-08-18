use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
};

use libc::c_int;
use serde::{Deserialize, Serialize};

use crate::buffer::guard::{BufferReadGuard, BufferWriteGuard};

use super::{disk::PrefixDiskManager, meta::FileTy};

#[derive(Deserialize, Serialize, Default)]
pub struct Dir(BTreeMap<String, (u64, FileTy)>);

pub type DirDiskManager = PrefixDiskManager<u64, Dir>;

pub struct DirReadGuard<'a, const S: usize>(BufferReadGuard<'a, u64, Dir, DirDiskManager, S>);

pub struct DirWriteGuard<'a, const S: usize>(BufferWriteGuard<'a, u64, Dir, DirDiskManager, S>);

impl Dir {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn insert(&mut self, name: String, ino: u64, ty: FileTy) {
        self.0.insert(name, (ino, ty));
    }

    pub fn remove(&mut self, name: &str) -> Option<(u64, FileTy)> {
        self.0.remove(name)
    }

    pub fn get(&self, name: &str) -> Result<&(u64, FileTy), c_int> {
        self.0.get(name).ok_or(libc::ENOENT)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &(u64, FileTy))> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }
}

impl<'a, const S: usize> DirReadGuard<'a, S> {
    pub fn new(guard: BufferReadGuard<'a, u64, Dir, DirDiskManager, S>) -> Self {
        Self(guard)
    }
}

impl<'a, const S: usize> Deref for DirReadGuard<'a, S> {
    type Target = Dir;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a, const S: usize> DirWriteGuard<'a, S> {
    pub fn new(guard: BufferWriteGuard<'a, u64, Dir, DirDiskManager, S>) -> Self {
        Self(guard)
    }

    pub async fn destroy(self) {
        self.0.destroy().await;
    }
}

impl<'a, const S: usize> Deref for DirWriteGuard<'a, S> {
    type Target = Dir;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a, const S: usize> DerefMut for DirWriteGuard<'a, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
