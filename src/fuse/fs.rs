use std::{path::PathBuf, time::SystemTime};

use fuser::FUSE_ROOT_ID;
use libc::c_int;
use tokio::fs::{self, OpenOptions};

use crate::{buffer::pool::BufferPool, subset::subset::Subset};

use super::{
    consistent::Consistent,
    dir::{Dir, DirDiskManager, DirReadGuard, DirWriteGuard},
    file::{FileReadGuard, FileWriteGuard},
    meta::{FileTy, Meta, MetaDiskManager, MetaReadGuard, MetaWriteGuard},
};

pub struct SyncFs<const S: usize> {
    /// The path to store the intermediate files for fuse.
    path: PathBuf,

    /// The next inode number.
    nino: Consistent<u64>,

    /// The buffer pool of metadata.
    mbp: BufferPool<u64, Meta, MetaDiskManager, S>,

    // The buffer pool of directory.
    dbp: BufferPool<u64, Dir, DirDiskManager, S>,

    fp: Subset<u64, S>,
}

impl<const S: usize> SyncFs<S> {
    pub async fn new(path: PathBuf, is_direct: bool) -> Self {
        let mbp = BufferPool::new(MetaDiskManager::new(path.join("metadata")).await, is_direct);
        let dbp = BufferPool::new(DirDiskManager::new(path.join("dir")).await, is_direct);

        fs::create_dir_all(path.join("file"))
            .await
            .expect("fail to create file dir");

        let root_meta_path = path.join("metadata").join(FUSE_ROOT_ID.to_string());
        let root_dir_path = path.join("dir").join(FUSE_ROOT_ID.to_string());
        if fs::metadata(root_meta_path).await.is_err() || fs::metadata(root_dir_path).await.is_err()
        {
            let mut meta = mbp
                .create(&FUSE_ROOT_ID)
                .await
                .expect("fail to create root meta");
            // it's promised that the sid and the ino of the root dir would both be `FUSE_ROOT_ID`
            meta.create(
                FUSE_ROOT_ID,
                FUSE_ROOT_ID,
                SystemTime::now(),
                FileTy::Dir,
                0o755,
                0,
                0,
            );

            let mut dir = dbp
                .create(&FUSE_ROOT_ID)
                .await
                .expect("fail to create root dir");
            dir.insert(".".to_string(), FUSE_ROOT_ID, FileTy::Dir);
            dir.insert("..".to_string(), FUSE_ROOT_ID, FileTy::Dir);
        }

        Self {
            path: path.clone(),
            nino: Consistent::new(path.join("nino"), FUSE_ROOT_ID + 1).await,
            mbp,
            dbp,
            fp: Subset::new(),
        }
    }

    async fn alloc_ino(&self) -> u64 {
        self.nino.apply(|x| x + 1).await
    }

    pub async fn read_meta(&self, ino: &u64) -> Result<MetaReadGuard<S>, c_int> {
        Ok(MetaReadGuard::new(self.mbp.read(ino).await?))
    }

    pub async fn write_meta(&self, ino: &u64) -> Result<MetaWriteGuard<S>, c_int> {
        Ok(MetaWriteGuard::new(self.mbp.write(ino).await?))
    }

    pub async fn read_dir(&self, ino: &u64) -> Result<DirReadGuard<S>, c_int> {
        Ok(DirReadGuard::new(self.dbp.read(ino).await?))
    }

    pub async fn write_dir(&self, ino: &u64) -> Result<DirWriteGuard<S>, c_int> {
        Ok(DirWriteGuard::new(self.dbp.write(ino).await?))
    }

    pub async fn read_file(&self, ino: &u64) -> Result<FileReadGuard<S>, c_int> {
        let path = self.path.join("file").join(ino.to_string());
        Ok(FileReadGuard::new(
            OpenOptions::new()
                .read(true)
                .open(&path)
                .await
                .map_err(|_| libc::EIO)?,
            path,
            self.fp.read(ino).await?,
        ))
    }

    pub async fn write_file(&self, ino: &u64) -> Result<FileWriteGuard<S>, c_int> {
        let path = self.path.join("file").join(ino.to_string());
        Ok(FileWriteGuard::new(
            OpenOptions::new()
                .write(true)
                .open(&path)
                .await
                .map_err(|_| libc::EIO)?,
            path,
            self.fp.write(ino).await?,
        ))
    }

    pub async fn create_dir(&self) -> Result<(u64, MetaWriteGuard<S>), c_int> {
        let ino = self.alloc_ino().await;
        self.dbp.create(&ino).await?;
        Ok((ino, MetaWriteGuard::new(self.mbp.create(&ino).await?)))
    }

    pub async fn create_file(&self) -> Result<(u64, MetaWriteGuard<S>), c_int> {
        let ino = self.alloc_ino().await;
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(self.path.join("file").join(ino.to_string()))
            .await
            .map_err(|_| libc::EIO)?;

        Ok((ino, MetaWriteGuard::new(self.mbp.create(&ino).await?)))
    }

    pub async fn access_dir(
        &self,
        ino: &u64,
    ) -> Result<(MetaWriteGuard<S>, DirReadGuard<S>), c_int> {
        Ok((self.write_meta(ino).await?, self.read_dir(ino).await?))
    }

    pub async fn modify_dir(
        &self,
        ino: &u64,
    ) -> Result<(MetaWriteGuard<S>, DirWriteGuard<S>), c_int> {
        Ok((self.write_meta(ino).await?, self.write_dir(ino).await?))
    }

    pub async fn access_file(
        &self,
        ino: &u64,
    ) -> Result<(MetaWriteGuard<S>, FileReadGuard<S>), c_int> {
        Ok((self.write_meta(ino).await?, self.read_file(ino).await?))
    }

    pub async fn modify_file(
        &self,
        ino: &u64,
    ) -> Result<(MetaWriteGuard<S>, FileWriteGuard<S>), c_int> {
        Ok((self.write_meta(ino).await?, self.write_file(ino).await?))
    }
}
