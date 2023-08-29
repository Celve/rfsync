use std::{
    cmp::{max, min},
    ffi::OsStr,
    ops::Deref,
    path::PathBuf,
    sync::Arc,
};

use fuser::FUSE_ROOT_ID;
use libc::c_int;
use tokio::fs;

use crate::{
    buffer::{disk::DiskManager, pool::BufferPool},
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
    pub mid: u64,
    nsid: Consistent<u64>,
    time: Consistent<u64>,
}

pub struct SyncTree<const S: usize>(Arc<SyncTreeInner<S>>);

impl<const S: usize> SyncTree<S> {
    pub async fn new(mid: u64, path: PathBuf, is_direct: bool) -> Self {
        let bp = BufferPool::new(SyncCellDiskManager::new(path.join("sync")).await, is_direct);

        let root_sc_path = path.join("sync").join(FUSE_ROOT_ID.to_string());
        if fs::metadata(root_sc_path).await.is_err() {
            let mut sc = bp
                .create(&FUSE_ROOT_ID)
                .await
                .expect("fail to create root sc");
            sc.empty(FUSE_ROOT_ID, FUSE_NONE_ID, PathBuf::new(), FileTy::Dir);
        }

        Self(Arc::new(SyncTreeInner {
            bp,
            mid,
            nsid: Consistent::new(path.join("nsid"), FUSE_ROOT_ID + 1).await,
            time: Consistent::new(path.join("time"), 1).await,
        }))
    }

    pub async fn forward(&self) -> u64 {
        self.time.apply(|x| x + 1).await
    }

    pub(super) async fn alloc_sid(&self) -> u64 {
        self.nsid.apply(|x| x + 1).await
    }

    /// Fetch the `SyncCell` if it's existing, otherwise create a new one and return.
    pub async fn create4parent(
        &self,
        parent: &mut SyncCell,
        name: &str,
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
            guard.init(sid, parent.sid, parent.path.join(name), parent.sync.clone());
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

    pub async fn get_id_by_path(&self, path: &PathBuf) -> Result<u64, c_int> {
        let names: Vec<_> = path.components().collect();
        let mut sid = FUSE_ROOT_ID;
        for name in names {
            let name = Self::osstr2str(name.as_os_str());
            let sc = self
                .read_by_id(&sid)
                .await
                .expect("fail to get the sync cell along the path");

            sid = if let Some(next_sid) = sc.children.get(name) {
                *next_sid
            } else {
                let mut sc = sc.upgrade().await;
                self.create4parent(&mut sc, name).await?.0
            };
        }

        Ok(sid)
    }

    pub async fn read_by_path(&self, path: &PathBuf) -> Result<SyncCellReadGuard<S>, c_int> {
        Ok(self.read_by_id(&self.get_id_by_path(path).await?).await?)
    }

    pub async fn write_by_path(&self, path: &PathBuf) -> Result<SyncCellWriteGuard<S>, c_int> {
        Ok(self.write_by_id(&self.get_id_by_path(path).await?).await?)
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
                psc.modif.union(&sc.modif, max);
                psc.sync.intersect(&sc.sync, min);
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
