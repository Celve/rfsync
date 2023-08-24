use std::{
    collections::{HashMap, VecDeque},
    ffi::OsStr,
    io::SeekFrom,
    net::SocketAddr,
    ops::Deref,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::SystemTime,
};

use async_recursion::async_recursion;
use fuser::FUSE_ROOT_ID;
use libc::c_int;
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{
        mpsc::{self, Sender},
        RwLock,
    },
};
use tracing::info;

use crate::{
    cell::{
        copy::{CopyCell, SyncOp},
        remote::RemoteCell,
        stge::CopyStge,
        sync::SyncCellWriteGuard,
        tree::SyncTree,
    },
    comm::{
        oneway::{Oneway, Request, Response},
        peer::Peer,
    },
    fuse::meta::FileTy,
};

use super::{
    dir::{DirReadGuard, DirWriteGuard},
    file::{FileReadGuard, FileWriteGuard},
    fs::SyncFs,
    meta::{Meta, MetaWriteGuard, FUSE_NONE_ID},
};

pub struct SyncServer<const S: usize> {
    pub(crate) fs: SyncFs<S>,
    pub(crate) tree: SyncTree<S>,
    pub(crate) stge: CopyStge,
    pub(crate) nfh: Arc<AtomicU64>,
    pub(crate) me: Peer,
    pub(crate) peers: Arc<RwLock<Vec<Peer>>>,
}

impl<const S: usize> SyncServer<S> {
    pub async fn new(me: Peer, path: PathBuf, is_direct: bool, peers: Vec<Peer>) -> Self {
        let fs = SyncFs::new(path.clone(), is_direct).await;
        let tree = SyncTree::new(me.id, path.clone(), is_direct).await;
        let stge = CopyStge::new(path).await;

        Self {
            fs,
            tree,
            stge,
            nfh: Arc::new(AtomicU64::new(1)),
            me,
            peers: Arc::new(RwLock::new(peers)),
        }
    }

    pub fn mid(&self) -> usize {
        self.me.id
    }

    pub fn addr(&self) -> SocketAddr {
        self.me.addr
    }

    fn osstr2str(osstr: &OsStr) -> &str {
        osstr.to_str().unwrap()
    }

    pub async fn sendaway(&self, peer: &Peer, path: &PathBuf) {
        loop {
            let res = Oneway::from(*peer)
                .request(&Request::SyncCell(self.me, path.clone()))
                .await;
            if let Response::Sync = res {
                break;
            }
        }
    }

    pub async fn join(&self, peer: Peer) {
        self.peers.write().await.push(peer.clone());
        let srv = self.clone();
        tokio::spawn(async move { srv.sendaway(&peer, &PathBuf::new()).await });
    }

    pub async fn leave(&self, target: &Peer) {
        let mut peers = self.peers.write().await;
        if let Some(i) = peers.iter().position(|peer| peer == target) {
            peers.remove(i);
        }
    }

    /// The broadcast is divided into two steps:
    /// - Broadcast it to its ancestors.
    /// - Broadcast it to server's peers.
    pub async fn broadcast(&self, sc: SyncCellWriteGuard<'_, S>) {
        info!("[modif] {:?} is modified to {:?}", sc.path, sc.modif);
        let path = sc.path.clone();
        let sid = sc.sid;
        drop(sc);
        let tree = self.tree.clone();
        tokio::spawn(async move { tree.sendup(&sid).await });

        let peers = self.peers.read().await;
        for peer in peers.iter() {
            let srv = self.clone();
            let peer = peer.clone();
            let path = path.clone();
            tokio::spawn(async move { srv.sendaway(&peer, &path).await });
        }
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
            let mut psc = self.tree.write_by_id(&pmeta.sid).await?;

            // modify children
            let (ino, mut meta) = self.fs.make_file().await?;

            // modify sync cell
            let sid = if !name.ends_with(".nosync") {
                let (sid, mut sc) = self.tree.create4parent(&mut psc, name).await?;
                drop(psc);
                sc.modify(self.tree.mid, self.tree.forward().await, FileTy::File);
                self.broadcast(sc).await;
                sid
            } else {
                FUSE_NONE_ID
            };

            // modify meta
            meta.create(ino, *parent, sid, now, FileTy::File, perm, uid, gid);

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
            let mut psc = self.tree.write_by_id(&pmeta.sid).await?;

            // modify children
            let (ino, mut meta) = self.fs.make_dir().await?;
            let mut dir = self.fs.write_dir(&ino).await?;

            let sid = if !name.ends_with(".nosync") {
                let (sid, mut sc) = self.tree.create4parent(&mut psc, name).await?;
                drop(psc);
                sc.modify(self.tree.mid, self.tree.forward().await, FileTy::Dir);
                self.broadcast(sc).await;
                sid
            } else {
                FUSE_NONE_ID
            };

            meta.create(ino, *parent, sid, now, FileTy::Dir, perm, uid, gid);
            dir.insert(".".to_string(), ino, FileTy::File);
            dir.insert("..".to_string(), *parent, FileTy::File);

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

            // modify sync cell
            if meta.sid != FUSE_NONE_ID {
                let mut sc = self.tree.write_by_id(&meta.sid).await?;
                sc.modify(self.tree.mid, self.tree.forward().await, FileTy::None);
                self.broadcast(sc).await;
            }

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
                // modify sync cell
                if meta.sid != FUSE_NONE_ID {
                    let mut sc = self.tree.write_by_id(&meta.sid).await?;
                    sc.modify(self.tree.mid, self.tree.forward().await, FileTy::None);
                    self.broadcast(sc).await;
                }

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
        if meta.sid != FUSE_NONE_ID {
            let mut sc = self.tree.write_by_id(&meta.sid).await?;
            sc.modify(self.mid(), self.tree.forward().await, FileTy::File);
            self.broadcast(sc).await;
        }

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
            if size != meta.size {
                meta.size = size;

                // modify file
                let file = self.fs.write_file(&ino).await?;
                file.set_len(size).await.map_err(|_| libc::EIO)?;

                if meta.sid != FUSE_NONE_ID {
                    let mut sc = self.tree.write_by_id(&meta.sid).await?;
                    sc.modify(self.mid(), self.tree.forward().await, FileTy::File);
                    self.broadcast(sc).await;
                }
            }
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

impl<const S: usize> SyncServer<S> {
    /// Return whether the synchronization is done.
    pub async fn sync(&self, ino: u64, rc: RemoteCell) -> Result<(), c_int> {
        let sid = {
            let meta = self.fs.read_meta(&ino).await?;
            meta.sid
        };

        let cc = CopyCell::make(sid, rc, self.tree.clone(), self.stge.clone()).await?;
        let (tx, _rx) = mpsc::channel(1);
        let _ = self.copy(ino, cc, tx).await;
        self.tree.sendup(&sid).await;

        Ok(())
    }

    #[async_recursion]
    pub async fn copy(&self, ino: u64, cc: CopyCell, tx: Sender<()>) -> Result<(), c_int> {
        match cc.sop {
            SyncOp::None => self.do_nothing_for_copy(ino, cc, tx).await,
            SyncOp::Copy => self.replace_for_copy(ino, cc, tx).await,
            SyncOp::Conflict => self.resolve_conflict_for_copy(ino, cc, tx).await,
            SyncOp::Recurse => self.recurse_down_for_copy(ino, cc, tx).await,
        }
    }

    pub async fn do_nothing_for_copy(
        &self,
        ino: u64,
        cc: CopyCell,
        tx: Sender<()>,
    ) -> Result<(), c_int> {
        info!("[sync] do nothing for {:?} {:?}", cc.path, cc.modif);
        let meta = self.fs.write_meta(&ino).await;
        tx.send(()).await.unwrap();
        drop(tx);

        let meta = meta?;
        let mut sc = self.tree.write_by_id(&meta.sid).await?;
        if sc.calc_sync_op(&cc) == cc.sop {
            sc.merge(&cc);
            Ok(())
        } else {
            drop(meta);
            drop(sc);
            self.sync(ino, RemoteCell::from_ow(cc.path, cc.oneway).await)
                .await
        }
    }

    pub async fn replace_for_copy(
        &self,
        ino: u64,
        cc: CopyCell,
        tx: Sender<()>,
    ) -> Result<(), c_int> {
        let meta = self.fs.write_meta(&ino).await;
        tx.send(()).await.unwrap();
        drop(tx);

        let mut meta = meta?;
        let mut sc = self.tree.write_by_id(&meta.sid).await?;
        if sc.calc_sync_op(&cc) == cc.sop {
            if sc.ty == FileTy::Dir {
                let pino = meta.parent;
                drop(meta);
                drop(sc);

                self.replace_dir_for_copy(ino, pino, cc).await
            } else {
                if cc.ty == FileTy::File {
                    sc.substituted(&cc);
                    let mut file = self.fs.write_file(&ino).await?;
                    let bytes = cc.read().await;
                    file.write(&bytes).await.map_err(|_| libc::EIO)?;

                    meta.modify(SystemTime::now());
                    meta.size = bytes.len() as u64;

                    Ok(())
                } else if cc.ty == FileTy::None {
                    sc.substituted(&cc);
                    self.fs.remove_file(&ino).await?;
                    meta.destroy().await;

                    Ok(())
                } else {
                    unreachable!()
                }
            }
        } else {
            drop(meta);
            drop(sc);
            self.sync(ino, RemoteCell::from_ow(cc.path, cc.oneway).await)
                .await
        }
    }

    pub async fn replace_dir_for_copy(
        &self,
        ino: u64,
        pino: u64,
        cc: CopyCell,
    ) -> Result<(), c_int> {
        let (mut pmeta, mut pdir) = self.fs.modify_dir(&pino).await?;
        let now = SystemTime::now();
        let (metas, scs, dirs, files) = self.write_whole_dir(&ino).await?;
        if scs.values().all(|sc| sc.calc_sync_op(&cc) == SyncOp::Copy) {
            pmeta.modify(now);

            let name = Self::osstr2str(cc.path.file_name().unwrap());
            if cc.ty == FileTy::None {
                pdir.remove(name);
            } else {
                let (ino, mut meta) = self.fs.make_file().await?;
                meta.create(
                    ino, ino, cc.sid, now, cc.ty, pmeta.perm, pmeta.uid, pmeta.gid,
                );
                pdir.insert(name.to_string(), ino, cc.ty);
            }

            for meta in metas.into_values() {
                meta.destroy().await;
            }

            for mut sc in scs.into_values() {
                sc.substituted(&cc);
            }

            for dir in dirs.into_values() {
                dir.destroy().await;
            }

            for file in files.into_values() {
                file.destroy().await;
            }

            Ok(())
        } else {
            self.sync(ino, RemoteCell::from_ow(cc.path, cc.oneway).await)
                .await
        }
    }

    pub async fn resolve_conflict_for_copy(
        &self,
        ino: u64,
        cc: CopyCell,
        tx: Sender<()>,
    ) -> Result<(), c_int> {
        let meta = self.fs.write_meta(&ino).await;
        tx.send(()).await.unwrap();
        drop(tx);

        let meta = meta?;
        let sc = self.tree.write_by_id(&meta.sid).await?;
        info!(
            "[sync] resolve conflict for {:?} due to {} and {}",
            cc.path,
            sc.deref(),
            cc
        );
        if sc.calc_sync_op(&cc) == cc.sop {
            let pino = meta.parent;
            drop(meta);
            drop(sc);

            let (mut pmeta, mut pdir) = self.fs.modify_dir(&pino).await?;
            let mut sc = self.tree.write_by_id(&cc.sid).await?;
            sc.merge(&cc);

            let name = format!("{}.nosync", Self::osstr2str(cc.path.file_name().unwrap()));
            let (ino, mut tmeta) = self.fs.make_file().await?;

            let mut cnt = 0;
            let mut newname = name.clone();
            while pdir.contains_key(&newname) {
                cnt += 1;
                newname = format!("{}{}", name, cnt);
            }
            pdir.insert(name, ino, FileTy::File);

            let now = SystemTime::now();
            tmeta.create(
                ino,
                pino,
                FUSE_NONE_ID,
                SystemTime::now(),
                FileTy::File,
                pmeta.perm,
                pmeta.uid,
                pmeta.gid,
            );
            pmeta.modify(now);

            let mut tfile = self.fs.write_file(&ino).await?;
            let bytes = cc.read().await;
            tfile.write(&bytes).await.map_err(|_| libc::EIO)?;
            tmeta.size = bytes.len() as u64;

            Ok(())
        } else {
            drop(meta);
            drop(sc);
            self.sync(ino, RemoteCell::from_ow(cc.path, cc.oneway).await)
                .await
        }
    }

    pub async fn recurse_down_for_copy(
        &self,
        ino: u64,
        cc: CopyCell,
        tx: Sender<()>,
    ) -> Result<(), c_int> {
        info!("[sync] recurse {:?} down", cc.path);
        let meta = self.fs.write_meta(&ino).await;
        tx.send(()).await.unwrap();
        drop(tx);

        let mut meta = meta?;
        let mut sc = self.tree.write_by_id(&meta.sid).await?;
        if sc.calc_sync_op(&cc) == cc.sop {
            sc.substituted(&cc);

            // convert file to dir if necessary
            let mut dir = if meta.ty == FileTy::File {
                meta.set_ty(FileTy::Dir);
                self.fs.destroy_file(&ino).await?;
                self.fs.create_dir(&ino).await?
            } else {
                self.fs.write_dir(&ino).await?
            };

            let now = SystemTime::now();
            let mut cnt = cc.children.len();
            if cnt > 0 {
                let (tx, mut rx) = mpsc::channel(cnt);
                for (name, cc) in cc.children {
                    let cino = if let Ok((cino, _)) = dir.get(&name) {
                        *cino
                    } else {
                        let (cino, mut cmeta) = if cc.ty == FileTy::File {
                            self.fs.make_file().await?
                        } else {
                            self.fs.make_dir().await?
                        };
                        cmeta.create(cino, ino, cc.sid, now, cc.ty, meta.perm, meta.uid, meta.gid);
                        dir.insert(name, cino, cc.ty);
                        cino
                    };
                    let srv = self.clone();
                    let tx = tx.clone();
                    tokio::spawn(async move { srv.copy(cino, cc, tx).await });
                }

                while let Some(_) = rx.recv().await {
                    cnt -= 1;
                    if cnt == 0 {
                        break;
                    }
                }
            }

            Ok(())
        } else {
            drop(meta);
            drop(sc);
            self.sync(ino, RemoteCell::from_ow(cc.path, cc.oneway).await)
                .await
        }
    }
}

impl<const S: usize> SyncServer<S> {
    pub async fn write_whole_dir(
        &self,
        ino: &u64,
    ) -> Result<
        (
            HashMap<u64, MetaWriteGuard<S>>,
            HashMap<u64, SyncCellWriteGuard<S>>,
            HashMap<u64, DirWriteGuard<S>>,
            HashMap<u64, FileWriteGuard<S>>,
        ),
        c_int,
    > {
        let mut metas = HashMap::new();
        let mut scs = HashMap::new();
        let mut dirs = HashMap::new();
        let mut files = HashMap::new();

        let mut queue = VecDeque::new();
        queue.push_back(*ino);

        while let Some(ino) = queue.pop_front() {
            let meta = self.fs.write_meta(&ino).await?;
            if meta.ty == FileTy::File {
                files.insert(ino, self.fs.write_file(&ino).await?);
            } else if meta.ty == FileTy::Dir {
                let dir = self.fs.write_dir(&ino).await?;
                for (_, (ino, _)) in dir.iter() {
                    queue.push_back(*ino);
                }
                dirs.insert(ino, dir);
            } else {
                panic!("file type is none");
            }

            let sc = self.tree.write_by_id(&meta.sid).await?;
            scs.insert(ino, sc);

            metas.insert(ino, meta);
        }

        Ok((metas, scs, dirs, files))
    }

    pub async fn read_dir_by_path(&self, path: &PathBuf) -> Result<DirReadGuard<S>, c_int> {
        let names: Vec<_> = path.components().collect();
        let mut dir = self.fs.read_dir(&FUSE_ROOT_ID).await?;
        for name in names {
            let name = Self::osstr2str(name.as_os_str());
            dir = self.fs.read_dir(&dir.get(name)?.0).await?;
        }

        Ok(dir)
    }

    pub async fn get_ino_by_path(&self, path: &PathBuf) -> Result<u64, c_int> {
        let mut parent_path = path.clone();
        parent_path.pop();
        let dir = self.read_dir_by_path(&parent_path).await?;
        let name = Self::osstr2str(path.file_name().unwrap());
        Ok(dir.get(name)?.0)
    }

    pub async fn get_existing_ino_by_path(&self, path: &PathBuf) -> (u64, PathBuf) {
        let ext = path.file_name().map(|name| Self::osstr2str(name));
        let mut parent_path = path.clone();
        parent_path.pop();
        let names: Vec<_> = parent_path.components().collect();
        let mut ino = FUSE_ROOT_ID;
        let mut real_path = PathBuf::new();
        for name in names {
            let dir = if let Ok(dir) = self.fs.read_dir(&ino).await {
                dir
            } else {
                return (ino, real_path);
            };

            let name = Self::osstr2str(name.as_os_str());
            ino = if let Ok(ino) = dir.get(name) {
                ino.0
            } else {
                return (ino, real_path);
            };
            real_path.push(name);
        }

        if let Some(ext) = ext {
            if let Ok(dir) = self.fs.read_dir(&ino).await {
                if let Ok((cino, _)) = dir.get(ext) {
                    ino = *cino;
                    real_path.push(ext);
                }
            }
        }

        (ino, real_path)
    }

    pub async fn read_file_by_path(&self, path: &PathBuf) -> Result<FileReadGuard<S>, c_int> {
        let ino = self.get_ino_by_path(path).await?;
        self.fs.read_file(&ino).await
    }
}

impl<const S: usize> Clone for SyncServer<S> {
    fn clone(&self) -> Self {
        Self {
            fs: self.fs.clone(),
            tree: self.tree.clone(),
            stge: self.stge.clone(),
            nfh: self.nfh.clone(),
            me: self.me.clone(),
            peers: self.peers.clone(),
        }
    }
}
