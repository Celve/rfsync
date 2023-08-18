use std::{ops::Deref, path::PathBuf, sync::Arc};

use fuser::FUSE_ROOT_ID;
use libc::c_int;
use tokio::fs;

use crate::{
    buffer::pool::BufferPool,
    cell::lean::LeanCell,
    fuse::{consistent::Consistent, meta::FileTy},
};

use super::{
    sync::{SyncCell, SyncCellDiskManager, SyncCellReadGuard, SyncCellWriteGuard},
    time::VecTime,
};

pub struct SyncTreeInner<const S: usize> {
    bp: BufferPool<u64, SyncCell, SyncCellDiskManager, S>,
    pub mid: usize,
    nsid: Consistent<u64>,
    time: Consistent<usize>,
}

pub struct SyncTree<const S: usize>(Arc<SyncTreeInner<S>>);

impl<const S: usize> SyncTree<S> {
    pub async fn new(mid: usize, path: PathBuf, is_direct: bool) -> Self {
        let bp = BufferPool::new(SyncCellDiskManager::new(path.join("sync")).await, is_direct);

        let root_sc_path = path.join("sync").join(FUSE_ROOT_ID.to_string());
        if fs::metadata(root_sc_path).await.is_err() {
            let mut sc = bp
                .create(&FUSE_ROOT_ID)
                .await
                .expect("fail to create root sc");
            sc.create(FUSE_ROOT_ID, PathBuf::new(), FileTy::Dir);
        }

        Self(Arc::new(SyncTreeInner {
            bp,
            mid,
            nsid: Consistent::new(path.join("nsid"), FUSE_ROOT_ID + 1).await,
            time: Consistent::new(path.join("time"), 1).await,
        }))
    }

    pub async fn forward(&self) -> usize {
        self.time.apply(|x| x + 1).await
    }

    async fn alloc_sid(&self) -> u64 {
        self.nsid.apply(|x| x + 1).await
    }

    pub async fn create(&self) -> Result<(u64, SyncCellWriteGuard<S>), c_int> {
        let sid = self.alloc_sid().await;
        Ok((
            sid,
            SyncCellWriteGuard::new(self.bp.create(&sid).await?, self.clone()),
        ))
    }

    pub async fn read(&self, sid: &u64) -> Result<SyncCellReadGuard<S>, c_int> {
        Ok(SyncCellReadGuard::new(
            self.bp.read(sid).await?,
            self.clone(),
        ))
    }

    pub async fn write(&self, sid: &u64) -> Result<SyncCellWriteGuard<S>, c_int> {
        Ok(SyncCellWriteGuard::new(
            self.bp.write(sid).await?,
            self.clone(),
        ))
    }

    pub async fn sendup(&self, lc: LeanCell) {
        let mut names: Vec<_> = lc.path.components().collect();
        names.pop();

        let mut sc = self
            .write(&FUSE_ROOT_ID)
            .await
            .expect("fail to get the sync cell along the path");
        sc.modif.merge_max(&lc.modif);
        for name in names {
            let name = name.as_os_str().to_str().unwrap();
            let sid = sc.children.get(name).expect("fail to get the sid");
            sc = self
                .write(sid)
                .await
                .expect("fail to get the sync cell along the path");
            sc.modif.merge_max(&lc.modif);
        }
    }
}

impl<const S: usize> Clone for SyncTree<S> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<const S: usize> Deref for SyncTree<S> {
    type Target = SyncTreeInner<S>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
