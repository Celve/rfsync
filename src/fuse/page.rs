use std::{
    ffi::c_int,
    fs::{File, OpenOptions},
    ops::{Deref, DerefMut},
};

use log::{info, warn};
use rustix::fs::FileExt;

use super::{buffer::Buffer, fs::SyncFsConfig, meta::PAGE_SIZE};

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

impl Default for Page {
    fn default() -> Self {
        Self {
            data: [0; PAGE_SIZE],
        }
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
