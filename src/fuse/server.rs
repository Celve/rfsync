use std::{
    io::SeekFrom,
    net::SocketAddr,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::SystemTime,
};

use libc::c_int;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::{cell::tree::SyncTree, comm::client::Client, fuse::meta::FileTy};

use super::{fs::SyncFs, meta::Meta};

pub struct SyncServer<const S: usize> {
    fs: SyncFs<S>,
    tree: SyncTree<S>,
    nfh: AtomicU64,
    client: Client,
}

impl<const S: usize> SyncServer<S> {
    pub async fn new(mid: usize, path: PathBuf, is_direct: bool, addr: SocketAddr) -> Self {
        let fs = SyncFs::new(path.clone(), is_direct).await;
        let tree = SyncTree::new(mid, path, is_direct).await;
        let client = Client::new(addr);

        Self {
            fs,
            tree,
            nfh: AtomicU64::new(1),
            client,
        }
    }

    pub fn mid(&self) -> usize {
        self.tree.mid
    }

    pub fn addr(&self) -> SocketAddr {
        self.client.addr
    }

    pub async fn mknod(
        &self,
        parent: &u64,
        name: &str,
        perm: u16,
        uid: u32,
        gid: u32,
    ) -> Result<Meta, c_int> {
        let (mut pmeta, mut pdir) = self.fs.modify_dir(parent).await?;
        let now = SystemTime::now();

        if !pdir.contains_key(name) {
            let psc = self.tree.read(&pmeta.sid).await?;

            // modify children
            let (ino, mut meta) = self.fs.create_file().await?;
            let (sid, mut sc) = self.tree.create().await?;
            meta.create(ino, sid, now, FileTy::File, perm, uid, gid);
            sc.create(sid, psc.path.join(name), FileTy::File);
            sc.modify(self.tree.mid, self.tree.forward().await, FileTy::Dir);

            // modify parent
            pmeta.modify(now);
            pdir.insert(name.to_string(), ino, FileTy::File);

            Ok(meta.clone())
        } else {
            pmeta.access(now);
            Err(libc::EEXIST)
        }
    }

    pub async fn mkdir(
        &self,
        parent: &u64,
        name: &str,
        perm: u16,
        uid: u32,
        gid: u32,
    ) -> Result<Meta, c_int> {
        let (mut pmeta, mut pdir) = self.fs.modify_dir(parent).await?;
        let now = SystemTime::now();

        if !pdir.contains_key(name) {
            let psc = self.tree.read(parent).await?;

            // modify children
            let (ino, mut meta) = self.fs.create_dir().await?;
            let mut dir = self.fs.write_dir(&ino).await?;
            let (sid, mut sc) = self.tree.create().await?;
            meta.create(ino, sid, now, FileTy::Dir, perm, uid, gid);
            dir.insert(".".to_string(), ino, FileTy::File);
            dir.insert("..".to_string(), *parent, FileTy::File);
            sc.create(sid, psc.path.join(name), FileTy::File);
            sc.modify(self.tree.mid, self.tree.forward().await, FileTy::Dir);

            // modify parent
            pmeta.modify(now);
            pdir.insert(name.to_string(), ino, FileTy::Dir);

            Ok(meta.clone())
        } else {
            pmeta.access(now);
            Err(libc::EEXIST)
        }
    }

    /// Currenlty, the permission is not checked.
    pub async fn unlink(&self, parent: &u64, name: &str) -> Result<(), c_int> {
        let (mut pmeta, mut pdir) = self.fs.modify_dir(parent).await?;
        let now = SystemTime::now();

        if let Some((ino, _)) = pdir.remove(name) {
            // modify children
            let (mut meta, file) = self.fs.modify_file(&ino).await?;
            let mut sc = self.tree.write(&meta.sid).await?;
            sc.modify(self.tree.mid, self.tree.forward().await, FileTy::None);
            if meta.unlink() {
                meta.destroy().await;
                file.destroy().await;
            }

            // modify parent
            pmeta.modify(now);

            Ok(())
        } else {
            pmeta.access(now);
            Err(libc::ENOENT)
        }
    }

    pub async fn rmdir(&self, parent: &u64, name: &str) -> Result<(), c_int> {
        let (mut pmeta, mut pdir) = self.fs.modify_dir(parent).await?;
        let now = SystemTime::now();

        if let Some((ino, _)) = pdir.remove(name) {
            // modify children
            let (mut meta, dir) = self.fs.modify_dir(&ino).await?;

            if dir.len() == 0 {
                let mut sc = self.tree.write(&meta.sid).await?;
                sc.modify(self.tree.mid, self.tree.forward().await, FileTy::None);
                if meta.unlink() {
                    meta.destroy().await;
                    dir.destroy().await;
                }

                // modify parent
                pmeta.modify(now);

                Ok(())
            } else {
                pmeta.access(now);
                Err(libc::ENOTDIR)
            }
        } else {
            pmeta.access(now);
            Err(libc::ENOENT)
        }
    }

    pub async fn open(&self, ino: &u64) -> Result<u64, c_int> {
        let mut meta = self.fs.write_meta(ino).await?;
        meta.access(SystemTime::now());
        meta.open();
        if meta.ty == FileTy::Dir {
            Err(libc::EISDIR)
        } else if meta.ty == FileTy::None {
            Err(libc::ENOENT)
        } else {
            Ok(self.nfh.fetch_add(1, Ordering::SeqCst))
        }
    }

    pub async fn read(&self, ino: &u64, offset: &i64, size: &u32) -> Result<Vec<u8>, c_int> {
        let (mut meta, mut file) = self.fs.access_file(ino).await?;
        let now = SystemTime::now();

        // modify meta
        meta.access(now);

        // read from disk
        let mut bytes = vec![0; *size as usize];
        file.seek(SeekFrom::Start(*offset as u64))
            .await
            .map_err(|_| libc::EIO)?;
        let len = file.read(&mut bytes).await.map_err(|_| libc::EIO)?;
        bytes.resize(len, 0);

        Ok(bytes)
    }

    pub async fn write(&self, ino: &u64, offset: &i64, bytes: &[u8]) -> Result<usize, c_int> {
        let (mut meta, mut file) = self.fs.modify_file(ino).await?;
        let mut sc = self.tree.write(&meta.sid).await?;
        let now = SystemTime::now();

        // read from disk
        file.seek(SeekFrom::Start(*offset as u64))
            .await
            .map_err(|_| libc::EIO)?;
        let len = file.write(bytes).await.map_err(|_| libc::EIO)?;

        // modify meta, never forget to modify the size param
        meta.access(now);
        meta.size = meta.size.max(*offset as u64 + len as u64);

        // modify sync cell
        sc.modify(self.mid(), self.tree.forward().await, FileTy::File);

        Ok(len)
    }

    pub async fn release(&self, ino: &u64) -> Result<(), c_int> {
        let mut meta = self.fs.write_meta(ino).await?;
        if meta.close() {
            let file = self.fs.write_file(ino).await?;
            meta.destroy().await;
            file.destroy().await;
        }
        Ok(())
    }

    pub async fn getattr(&self, ino: &u64) -> Result<Meta, c_int> {
        let mut meta = self.fs.write_meta(ino).await?;
        meta.access(SystemTime::now());
        Ok(meta.clone())
    }

    pub async fn setattr(
        &self,
        ino: &u64,
        size: Option<u64>,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
        ctime: Option<SystemTime>,
        crtime: Option<SystemTime>,
    ) -> Result<Meta, c_int> {
        let mut meta = self.fs.write_meta(&ino).await?;

        if let Some(size) = size {
            // modify inode
            meta.size = size;

            // modify file
            let file = self.fs.write_file(&ino).await?;
            file.set_len(size).await.map_err(|_| libc::EIO)?;
        }

        if let Some(atime) = atime {
            meta.atime = atime;
        }

        if let Some(mtime) = mtime {
            meta.mtime = mtime;
        }

        if let Some(ctime) = ctime {
            meta.ctime = ctime;
        }

        if let Some(crtime) = crtime {
            meta.crtime = crtime;
        }

        Ok(meta.clone())
    }

    pub async fn lookup(&self, parent: &u64, name: &str) -> Result<Meta, c_int> {
        let (mut meta, dir) = self.fs.access_dir(parent).await?;
        meta.access(SystemTime::now());

        let (ino, _) = dir.get(name)?;
        Ok(self.fs.read_meta(ino).await?.clone())
    }

    pub async fn readdir(
        &self,
        ino: &u64,
        offset: i64,
    ) -> Result<Vec<(u64, i64, FileTy, String)>, c_int> {
        let (mut meta, dir) = self.fs.modify_dir(ino).await?;
        meta.access(SystemTime::now());
        Ok(dir
            .iter()
            .skip(offset as usize)
            .enumerate()
            .map(|(i, (str, (ino, ty)))| (*ino, offset + i as i64 + 1, ty.clone(), str.clone()))
            .collect())
    }
}
