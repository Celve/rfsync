use std::{ffi::OsStr, ops::Deref, path::PathBuf, sync::Arc};

use fuser::FUSE_ROOT_ID;
use libc::c_int;
use tokio::fs;

use crate::{
    buffer::pool::BufferPool,
    fuse::{
        consistent::Consistent,
        meta::{FileTy, FUSE_NONE_ID},
    },
};

use super::{
    sync::{SyncCell, SyncCellDiskManager, SyncCellReadGuard, SyncCellWriteGuard},
    time::VecTime,
};

pub struct SyncTreeInner<const S: usize> {
    pub(super) bp: BufferPool<u64, SyncCell, SyncCellDiskManager, S>,
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
            sc.create(FUSE_ROOT_ID, FUSE_NONE_ID, PathBuf::new(), FileTy::Dir);
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

    pub(super) async fn alloc_sid(&self) -> u64 {
        self.nsid.apply(|x| x + 1).await
    }

    pub async fn create4parent(
        &self,
        parent: &mut SyncCell,
        name: &str,
        ty: FileTy,
    ) -> Result<(u64, SyncCellWriteGuard<S>), c_int> {
        Ok(if let Some(sid) = parent.children.get(name) {
            (
                *sid,
                SyncCellWriteGuard::new(self.bp.write(&sid).await?, self.clone()),
            )
        } else {
            let sid = self.alloc_sid().await;
            parent.children.insert(name.to_string(), sid);
            let mut guard = SyncCellWriteGuard::new(self.bp.create(&sid).await?, self.clone());
            guard.create(sid, parent.sid, parent.path.join(name), ty);
            (sid, guard)
        })
    }

    pub async fn read_by_id(&self, sid: &u64) -> Result<SyncCellReadGuard<S>, c_int> {
        Ok(SyncCellReadGuard::new(
            self.bp.read(sid).await?,
            self.clone(),
        ))
    }

    pub async fn write_by_id(&self, sid: &u64) -> Result<SyncCellWriteGuard<S>, c_int> {
        Ok(SyncCellWriteGuard::new(
            self.bp.write(sid).await?,
            self.clone(),
        ))
    }

    fn osstr2str(osstr: &OsStr) -> &str {
        osstr.to_str().unwrap()
    }

    pub async fn read_by_path(&self, path: &PathBuf) -> Result<SyncCellReadGuard<S>, c_int> {
        let names: Vec<_> = path.components().collect();

        let mut sc = self
            .read_by_id(&FUSE_ROOT_ID)
            .await
            .expect("fail to get the sync cell along the path");
        for name in names {
            let name = Self::osstr2str(name.as_os_str());
            let sid = sc.children.get(name).ok_or(libc::ENOENT)?;
            sc = self
                .read_by_id(sid)
                .await
                .expect("fail to get the sync cell along the path");
        }

        Ok(sc)
    }

    pub async fn write_by_path(&self, path: &PathBuf) -> Result<SyncCellWriteGuard<S>, c_int> {
        let mut names: Vec<_> = path.components().collect();
        let last = names.pop();

        let mut sc = self
            .read_by_id(&FUSE_ROOT_ID)
            .await
            .expect("fail to get the sync cell along the path");
        for name in names {
            let name = Self::osstr2str(name.as_os_str());
            let sid = sc.children.get(name).ok_or(libc::ENOENT)?;
            sc = self
                .read_by_id(sid)
                .await
                .expect("fail to get the sync cell along the path");
        }

        let name = Self::osstr2str(last.unwrap().as_os_str());
        let sid = sc.children.get(name).ok_or(libc::ENOENT)?;
        Ok(SyncCellWriteGuard::new(
            self.bp.write(sid).await?,
            self.clone(),
        ))
    }

    pub async fn sendup(&self, sid: &u64) {
        let mut psid = self.read_by_id(sid).await.unwrap().parent;
        while psid != FUSE_NONE_ID {
            let mut psc = self
                .write_by_id(&psid)
                .await
                .expect("fail to get the sync cell along the path");
            psc.modif = VecTime::new();
            psc.sync = VecTime::new();
            let children: Vec<_> = psc.children.values().map(|v| *v).collect();
            for sid in children {
                let sc = self
                    .read_by_id(&sid)
                    .await
                    .expect("fail to get the sync cell along the path");
                psc.modif.merge_max(&sc.modif);
                psc.sync.merge_min(&sc.sync);
            }
            psid = psc.parent;
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
