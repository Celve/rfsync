use std::{
    fs::{self, File},
    ops::{Deref, DerefMut},
    time::SystemTime,
};

use fuser::{FileAttr, FileType};
use libc::c_int;
use log::warn;
use rustix::fs::FileExt;
use serde::{Deserialize, Serialize};

use super::{buffer::Buffer, fs::SyncFsConfig};

pub const PAGE_SIZE: usize = 4096;

/// Called `FileTy` to distinguish with other `FileType` defined in different crates.
#[derive(Deserialize, Serialize, Default, Clone, Copy, PartialEq, Eq)]
pub enum FileTy {
    #[default]
    File,

    Dir,
}

/// The data representation on disk, used for serialization.
pub struct Metadata {
    pub nid: u64,
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

    fhc: usize,
}

#[derive(Default)]
pub struct MetadataHandle {
    /// Whether the content of the handle is modified.
    is_dirty: bool,

    // The memory representation of metadata.
    metadata: Metadata,
}

impl FileTy {
    pub fn from_mode(mode: u32) -> Self {
        match mode & libc::S_IFMT {
            libc::S_IFREG => Self::File,
            libc::S_IFDIR => Self::Dir,
            _ => unimplemented!(),
        }
    }
}

impl Metadata {
    pub fn new(
        nid: u64,
        size: u64,
        atime: SystemTime,
        mtime: SystemTime,
        ctime: SystemTime,
        crtime: SystemTime,
        ty: FileTy,
        perm: u16,
        nlink: u32,
        uid: u32,
        gid: u32,
    ) -> Self {
        Self {
            nid,
            size,
            atime: crtime,
            mtime: crtime,
            ctime: crtime,
            crtime,
            ty,
            perm,
            nlink,
            uid,
            gid,
            fhc: 0,
        }
    }

    pub fn create(&mut self, time: SystemTime, ty: FileTy, perm: u16, uid: u32, gid: u32) {
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

    pub fn unlink(&mut self, config: &SyncFsConfig) {
        self.nlink -= 1;

        if self.nlink == 0 && self.fhc == 0 {
            fs::remove_file(config.inode_path(self.nid)).unwrap();
        }
    }

    pub fn open(&mut self) {
        self.fhc += 1;
    }

    pub fn close(&mut self, config: &SyncFsConfig) {
        self.fhc -= 1;
        if self.nlink == 0 && self.fhc == 0 {
            fs::remove_file(config.dnode_path(self.nid)).unwrap();
        }
    }

    pub fn into_attr(&self) -> FileAttr {
        FileAttr {
            ino: self.nid,
            size: self.size,
            blocks: (self.size + PAGE_SIZE as u64 - 1) / PAGE_SIZE as u64,
            atime: self.atime,
            mtime: self.mtime,
            ctime: self.ctime,
            crtime: self.crtime,
            kind: match self.ty {
                FileTy::File => fuser::FileType::RegularFile,
                FileTy::Dir => fuser::FileType::Directory,
            },
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

impl MetadataHandle {
    pub fn new(metadata: Metadata) -> Self {
        Self {
            is_dirty: false,
            metadata,
        }
    }
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            nid: 0,
            size: 0,
            atime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            ctime: SystemTime::UNIX_EPOCH,
            crtime: SystemTime::UNIX_EPOCH,
            ty: FileTy::default(),
            perm: 0o777,
            nlink: 0,
            uid: 0,
            gid: 0,
            fhc: 0,
        }
    }
}

impl Buffer for MetadataHandle {
    type Key = u64;
    type Value = Metadata;

    fn empty(config: &SyncFsConfig, key: &Self::Key) -> Self {
        let mut value = Self::Value::default();
        value.nid = *key;
        let buffer = Self::new(value);
        let bytes: &[u8; std::mem::size_of::<Self::Value>()] =
            unsafe { std::mem::transmute(buffer.value()) };
        let path = config.inode_path(*key);
        fs::write(&path, bytes).unwrap();
        buffer
    }

    fn from_fs(config: &SyncFsConfig, key: &Self::Key) -> Result<Self, c_int> {
        let path = config.inode_path(*key);
        if let Ok(file) = File::open(&path) {
            // do conversion
            let mut bytes = [0; std::mem::size_of::<Self::Value>()];
            file.read_at(&mut bytes, 0).unwrap();
            let value: Self::Value = unsafe { std::mem::transmute(bytes) };

            Ok(Self::new(value))
        } else {
            warn!("[buffer] failed to open metadata {}", key);
            Err(libc::ENOENT)
        }
    }

    fn dirty(&mut self) {
        self.is_dirty = true;
    }

    fn fsync(&mut self, config: &SyncFsConfig) {
        if self.is_dirty {
            let path = config.inode_path(self.key());
            let bytes: &[u8; std::mem::size_of::<Self::Value>()] =
                unsafe { std::mem::transmute(self.value()) };

            // this is correct due to the fact that nid is not recycled
            if let Err(err) = fs::write(&path, bytes) && err.kind() != std::io::ErrorKind::NotFound {
                panic!("fsyncing dents failed: {}", err);
            }
            self.is_dirty = false;
        }
    }

    fn key(&self) -> Self::Key {
        self.metadata.nid
    }

    fn value(&self) -> &Self::Value {
        &self.metadata
    }
}

impl Deref for MetadataHandle {
    type Target = Metadata;

    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}

impl DerefMut for MetadataHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.metadata
    }
}

impl From<FileType> for FileTy {
    fn from(value: FileType) -> Self {
        match value {
            FileType::Directory => FileTy::Dir,
            FileType::RegularFile => FileTy::File,
            _ => todo!(),
        }
    }
}

impl Into<FileType> for FileTy {
    fn into(self) -> FileType {
        match self {
            FileTy::File => FileType::RegularFile,
            FileTy::Dir => FileType::Directory,
        }
    }
}
