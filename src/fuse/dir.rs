use std::{
    collections::BTreeMap,
    fs::{self, File},
    ops::{Deref, DerefMut},
};

use libc::c_int;
use log::warn;
use serde::{Deserialize, Serialize};

use super::{buffer::Buffer, fs::SyncFsConfig, meta::FileTy};

#[derive(Default, Deserialize, Serialize)]
pub struct Dir {
    entries: BTreeMap<String, (u64, FileTy)>,
}

#[derive(Default)]
pub struct DirHandle {
    /// The inode id of the directory.
    nid: u64,

    /// Whether the content of the handle is modified.
    is_dirty: bool,

    /// The memory representation of directory entries.
    dir: Dir,
}

impl Dir {
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

impl DirHandle {
    pub fn new(nid: u64, dir: Dir) -> Self {
        Self {
            nid,
            is_dirty: false,
            dir,
        }
    }
}

impl Deref for DirHandle {
    type Target = Dir;

    fn deref(&self) -> &Self::Target {
        &self.dir
    }
}

impl DerefMut for DirHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.dir
    }
}

impl Buffer for DirHandle {
    type Key = u64;
    type Value = Dir;

    fn empty(config: &SyncFsConfig, key: &Self::Key) -> Self {
        let value = Self::Value::default();
        let path = config.dir_path(*key);
        let buffer = Self::new(*key, value);
        let bytes = bincode::serialize(buffer.value()).unwrap();
        // This is correct due to the fact that nid is not recycled.
        fs::write(&path, bytes).unwrap();
        buffer
    }

    fn from_fs(config: &SyncFsConfig, key: &Self::Key) -> Result<Self, c_int> {
        let path = config.dir_path(*key);
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
            let path = config.dir_path(self.key());
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
        &self.dir
    }
}
