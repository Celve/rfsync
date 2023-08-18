use std::{
    ops::{Deref, DerefMut},
    time::SystemTime,
};

use fuser::{FileAttr, FileType};
use serde::{Deserialize, Serialize};

use crate::buffer::guard::{BufferReadGuard, BufferWriteGuard};

use super::disk::PrefixDiskManager;

pub const PAGE_SIZE: usize = 4096;

#[derive(Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
pub enum FileTy {
    #[default]
    None,
    File,
    Dir,
}

/// When `Meta` is inited as `default`, all of its time would be assigned with `UNIX_EPOCH` constant.
#[derive(Deserialize, Serialize, Clone)]
pub struct Meta {
    pub ino: u64,
    pub sid: u64,
    pub size: u64,

    pub atime: SystemTime,

    /// "Modify" is the timestamp of the last time the file's content has been modified.
    pub mtime: SystemTime,

    /// "Change" is the timestamp of the last time the file's inode has been changed.
    pub ctime: SystemTime,

    pub crtime: SystemTime,

    pub ty: FileTy,
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,

    pub fhc: usize,
}

pub type MetaDiskManager = PrefixDiskManager<u64, Meta>;

pub struct MetaReadGuard<'a, const S: usize>(BufferReadGuard<'a, u64, Meta, MetaDiskManager, S>);

pub struct MetaWriteGuard<'a, const S: usize>(BufferWriteGuard<'a, u64, Meta, MetaDiskManager, S>);

impl Into<FileType> for FileTy {
    fn into(self) -> FileType {
        match self {
            FileTy::File => FileType::RegularFile,
            FileTy::Dir => FileType::Directory,
            FileTy::None => panic!("file type is none"),
        }
    }
}

impl Meta {
    pub fn create(
        &mut self,
        ino: u64,
        sid: u64,
        time: SystemTime,
        ty: FileTy,
        perm: u16,
        uid: u32,
        gid: u32,
    ) {
        self.ino = ino;
        self.sid = sid;
        self.atime = time;
        self.mtime = time;
        self.ctime = time;
        self.crtime = time;
        self.ty = ty;
        self.perm = perm;
        self.nlink = 1;
        self.uid = uid;
        self.gid = gid;
    }

    pub fn modify(&mut self, time: SystemTime) {
        self.atime = time;
        self.mtime = time;
    }

    pub fn change(&mut self, time: SystemTime) {
        self.ctime = time;
    }

    pub fn access(&mut self, time: SystemTime) {
        self.atime = time;
    }

    /// Return whether the file should be eliminate.
    pub fn unlink(&mut self) -> bool {
        self.nlink -= 1;

        if self.nlink == 0 {
            self.ty = FileTy::None;
        }

        self.nlink == 0 && self.fhc == 0
    }

    pub fn open(&mut self) {
        self.fhc += 1;
    }

    /// Return whether the file should be eliminate.
    pub fn close(&mut self) -> bool {
        self.fhc -= 1;
        self.nlink == 0 && self.fhc == 0
    }
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            ino: Default::default(),
            sid: Default::default(),
            size: Default::default(),
            atime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            ctime: SystemTime::UNIX_EPOCH,
            crtime: SystemTime::UNIX_EPOCH,
            ty: Default::default(),
            perm: Default::default(),
            nlink: Default::default(),
            uid: Default::default(),
            gid: Default::default(),
            fhc: Default::default(),
        }
    }
}

impl Into<FileAttr> for Meta {
    fn into(self) -> FileAttr {
        FileAttr {
            ino: self.ino,
            size: self.size,
            blocks: (self.size + PAGE_SIZE as u64 - 1) / PAGE_SIZE as u64,
            atime: self.atime,
            mtime: self.mtime,
            ctime: self.ctime,
            crtime: self.crtime,
            kind: self.ty.into(),
            perm: self.perm,
            nlink: self.nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: PAGE_SIZE as u32,
            flags: 0,
        }
    }
}

impl<'a, const S: usize> MetaReadGuard<'a, S> {
    pub fn new(guard: BufferReadGuard<'a, u64, Meta, MetaDiskManager, S>) -> Self {
        Self(guard)
    }

    pub async fn upgrade(self) -> MetaWriteGuard<'a, S> {
        MetaWriteGuard(self.0.upgrade().await)
    }
}

impl<'a, const S: usize> Deref for MetaReadGuard<'a, S> {
    type Target = Meta;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a, const S: usize> MetaWriteGuard<'a, S> {
    pub fn new(guard: BufferWriteGuard<'a, u64, Meta, MetaDiskManager, S>) -> Self {
        Self(guard)
    }

    pub async fn destroy(self) {
        self.0.destroy().await;
    }
}

impl<'a, const S: usize> Deref for MetaWriteGuard<'a, S> {
    type Target = Meta;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a, const S: usize> DerefMut for MetaWriteGuard<'a, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
