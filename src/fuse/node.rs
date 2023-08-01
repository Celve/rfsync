use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    ops::{Deref, DerefMut},
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::SystemTime,
};

use fuser::{FileAttr, FileType};
use libc::c_int;
use log::{debug, info, warn};
use rustix::fs::FileExt;
use serde::{Deserialize, Serialize};

use super::{buffer::Buffer, sync::SyncFsConfig};

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

#[derive(Default, Deserialize, Serialize)]
pub struct Dents {
    entries: BTreeMap<String, (u64, FileTy)>,
}

#[derive(Default)]
pub struct DentsHandle {
    /// The inode id of the directory.
    nid: u64,

    /// Whether the content of the handle is modified.
    is_dirty: bool,

    /// The memory representation of directory entries.
    dents: Dents,
}

pub struct Page {
    data: [u8; PAGE_SIZE],
}

/// Now it's a bit of awkward to deal with deleting the file.
/// Because the nid recycling is not supported, the feature could be used to deal with fsyncing wrongly with the deleted file.
/// However, nid recycling would be supported someday and there should be some other method to deal with it.
///
/// Currently, there could be basically several straight-forward methods:
/// 1. Maintain a tracer in the `BufferPool` to record all buffer a file has.
/// 2. Use the len in metadata to check whether the fsync is valid.
/// 3. Create a fresh new data structure to deal with page cache.
#[derive(Default)]
pub struct PageHandle {
    /// The dnode id of the file that containing the page.
    nid: u64,

    /// The offset of the page, counted in page number.
    offset: u64,

    /// Whether the content of the handle is modified.
    is_dirty: bool,

    /// The size that has been used in the current page.
    size: usize,

    /// The memory representation of Page.
    page: Page,
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

impl Dents {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, name: String, nid: u64, ty: FileTy) {
        self.entries.insert(name, (nid, ty));
    }

    pub fn remove(&mut self, name: &String) -> Option<(u64, FileTy)> {
        self.entries.remove(name)
    }

    pub fn get(&self, name: &str) -> Result<&(u64, FileTy), c_int> {
        self.entries.get(name).ok_or(libc::ENOENT)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &(u64, FileTy))> {
        self.entries.iter()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn contains_key(&self, key: &String) -> bool {
        self.entries.contains_key(key)
    }
}

impl DentsHandle {
    pub fn new(nid: u64, dents: Dents) -> Self {
        Self {
            nid,
            is_dirty: false,
            dents,
        }
    }
}

impl PageHandle {
    pub fn new(nid: u64, offset: u64, size: usize, page: Page) -> Self {
        Self {
            nid,
            offset,
            is_dirty: false,
            size,
            page,
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

impl Default for Page {
    fn default() -> Self {
        Self {
            data: [0; PAGE_SIZE],
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

impl Buffer for DentsHandle {
    type Key = u64;
    type Value = Dents;

    fn empty(config: &SyncFsConfig, key: &Self::Key) -> Self {
        let value = Self::Value::default();
        let path = config.dnode_path(*key);
        let buffer = Self::new(*key, value);
        let bytes = bincode::serialize(buffer.value()).unwrap();
        // This is correct due to the fact that nid is not recycled.
        fs::write(&path, bytes).unwrap();
        buffer
    }

    fn from_fs(config: &SyncFsConfig, key: &Self::Key) -> Result<Self, c_int> {
        let path = config.dnode_path(*key);
        if let Ok(file) = File::open(&path) {
            // do conversion
            let value: Self::Value = bincode::deserialize_from(file).unwrap();

            Ok(Self::new(*key, value))
        } else {
            warn!("[buffer] failed to open directory {}", key);
            Err(libc::ENOENT)
        }
    }

    fn dirty(&mut self) {
        self.is_dirty = true;
    }

    fn fsync(&mut self, config: &SyncFsConfig) {
        if self.is_dirty {
            let path = config.dnode_path(self.key());
            let bytes = bincode::serialize(self.value()).unwrap();

            // this is correct due to the fact that nid is not recycled
            if let Err(err) = fs::write(&path, &bytes) && err.kind() != std::io::ErrorKind::NotFound {
                panic!("fsyncing dents failed: {}", err);
            }
            self.is_dirty = false;
        }
    }

    fn key(&self) -> Self::Key {
        self.nid
    }

    fn value(&self) -> &Self::Value {
        &self.dents
    }
}

impl Buffer for PageHandle {
    type Key = (u64, u64);
    type Value = Page;

    fn empty(config: &SyncFsConfig, key: &Self::Key) -> Self {
        let (nid, offset) = key;
        let value = Self::Value::default();
        let buffer = Self::new(*nid, *offset, 0, value);
        let bytes: &[u8; std::mem::size_of::<Self::Value>()] =
            unsafe { std::mem::transmute(buffer.value()) };

        let path = config.dnode_path(*nid);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.write_at(bytes, offset * PAGE_SIZE as u64).unwrap();
        buffer
    }

    fn from_fs(config: &SyncFsConfig, key: &Self::Key) -> Result<Self, c_int> {
        let (nid, offset) = key;
        let path = config.dnode_path(*nid);
        if let Ok(file) = File::open(&path) {
            // do conversion
            let mut bytes = [0; std::mem::size_of::<Self::Value>()];
            let size = file.read_at(&mut bytes, 0).unwrap();
            let value: Self::Value = unsafe { std::mem::transmute(bytes) };

            Ok(Self::new(*nid, *offset, size, value))
        } else {
            warn!("[buffer] failed to open page {:?}", key);
            Err(libc::ENOENT)
        }
    }

    fn dirty(&mut self) {
        self.is_dirty = true;
    }

    fn fsync(&mut self, config: &SyncFsConfig) {
        if self.is_dirty {
            let (nid, offset) = self.key();
            let path = config.dnode_path(nid);
            info!("[file] fsync page ({}, {}) to {:?}", nid, offset, path);

            let bytes: &[u8; std::mem::size_of::<Self::Value>()] =
                unsafe { std::mem::transmute(self.value()) };

            // this is correct due to the fact that nid is not recycled
            if let Ok(file) = File::create(&path) {
                file.write_at(bytes, offset * PAGE_SIZE as u64).unwrap();
            }
            self.is_dirty = false;
        }
    }

    fn key(&self) -> Self::Key {
        (self.nid, self.offset)
    }

    fn value(&self) -> &Self::Value {
        &self.page
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

impl Deref for DentsHandle {
    type Target = Dents;

    fn deref(&self) -> &Self::Target {
        &self.dents
    }
}

impl DerefMut for DentsHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.dents
    }
}

impl Deref for Page {
    type Target = [u8; PAGE_SIZE];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl DerefMut for Page {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl Deref for PageHandle {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl DerefMut for PageHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.page
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
