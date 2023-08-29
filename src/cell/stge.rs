use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use tokio::{fs::File, io::{BufReader, BufWriter}};

use crate::{buffer::disk::DiskManager, disk::serde::PrefixSerdeDiskManager, rsync::inst::Inst};

pub type CopyCellDiskManager = PrefixSerdeDiskManager<u64, Vec<Inst>>;

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

    pub async fn read(&self, cid: &u64) -> Vec<Inst> {
        self.dm.read(cid).await
    }

    pub async fn write(&self, cid: &u64, insts: &Vec<Inst>) {
        self.dm.write(cid, insts).await
    }

    pub async fn remove(&self, cid: &u64) {
        self.dm.remove(cid).await;
    }

    pub async fn read_as_buf_reader(&self, cid: &u64) -> BufReader<File> {
        BufReader::new(self.dm.read_as_file(cid).await)
    }

    pub async fn write_as_stream(&self, cid: &u64) -> BufWriter<File> {
        BufWriter::new(self.dm.write_as_file(cid).await)
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
