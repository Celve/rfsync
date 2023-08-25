use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use crate::{
    buffer::disk::DiskManager, disk::serde::PrefixSerdeDiskManager, rsync::inst::InstList,
};

pub type CopyCellDiskManager = PrefixSerdeDiskManager<u64, InstList>;

pub struct CopyStge {
    ncid: Arc<AtomicU64>,
    dm: CopyCellDiskManager,
}

impl CopyStge {
    pub async fn new(path: PathBuf) -> Self {
        Self {
            ncid: Arc::new(AtomicU64::new(1)),
            dm: CopyCellDiskManager::new(path.join("copy")).await,
        }
    }

    pub fn alloc_cid(&self) -> u64 {
        self.ncid.fetch_add(1, Ordering::SeqCst)
    }

    pub async fn create(&self, cid: &u64) {
        self.dm.create(cid).await;
    }

    pub async fn read(&self, cid: &u64) -> InstList {
        self.dm.read(cid).await
    }

    pub async fn write(&self, cid: &u64, insts: &InstList) {
        self.dm.write(cid, insts).await
    }

    pub async fn remove(&self, cid: &u64) {
        self.dm.remove(cid).await;
    }
}

impl Clone for CopyStge {
    fn clone(&self) -> Self {
        Self {
            ncid: self.ncid.clone(),
            dm: self.dm.clone(),
        }
    }
}
