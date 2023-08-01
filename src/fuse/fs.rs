use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{atomic::AtomicUsize, Mutex, RwLock},
    time::{Duration, SystemTime},
};
use std::{ffi::OsStr, fs};

use fuser::{self, FileAttr, Filesystem};
use libc::{self, c_int};
use log::{debug, error, warn};

use super::{
    buffer::BufferPool,
    dir::DirHandle,
    meta::{FileTy, MetadataHandle},
};

/// The node id of root directory.
const ROOT_DIR_NID: u64 = 1;

#[derive(Clone)]
pub struct SyncFsConfig {
    fs_path: PathBuf,
    is_direct: bool,
}

pub struct FuseEntry {
    ttl: Duration,
    attr: FileAttr,
    generation: u64,
}

pub struct FuseAttr {
    ttl: Duration,
    attr: FileAttr,
}

type FuseDir = Vec<(u64, i64, FileTy, String)>;

/// `SyncFs` is a specially designed fuse for file system synchronization.
///
/// The basic design is derived from the `SimpleFS` example in the `fuser` crate.
/// The `SyncFs` seperates all its nodes to inodes and dnodes,
/// which are simply stored as a file in an specified directory in the local file system.
/// Every inode points to a correspond dnode which stores the data of the file,
/// while the inode itself stores the metadata of the file.
/// To simplify the mapping overhead, the inode and dnode shares the same id, called node id.
///
/// The file structure inside the data directory could be summarize as **super block**, **inodes**, and **dnodes**.
///
/// The **super block**, in the current design, only contains the next inode id to be allocated,
/// which would be synced each time the allocation happens.
///
/// The **inodes** are stored inside the inodes directory, while the **dnodes** are stored inside the dnodes directory.
///
/// For compability reason, custom struct defined in `rfsync` crate may not be adopted to enhance semantics.
///
/// A simple description toward the locking rule: always lock the metadata first.
pub struct SyncFs {
    /// The path to stores the inodes and dnodes.
    // data_path: PathBuf,
    config: SyncFsConfig,

    /// The next inode id to be allocated.
    /// Whether to use a bitmap is not decided yet.
    nnid: Mutex<u64>,

    /// The next file handle to be allocated.
    /// It doesn't need to be persisted because it is only used in the current process.
    next_file_handle: AtomicUsize,

    /// The cache for metadata.
    mbp: BufferPool<MetadataHandle>,

    /// The cache for dir entries.
    dbp: BufferPool<DirHandle>,

    /// The pool of latches for files. Currently, any write towards the file would lock it first.
    lp: RwLock<HashMap<u64, RwLock<()>>>,
}

impl SyncFsConfig {
    pub fn new(fs_path: PathBuf, is_direct: bool) -> Self {
        Self { fs_path, is_direct }
    }

    /// Return the true path of the inode with the given id.
    /// In the current design, the iid is the same as the did.
    pub(super) fn meta_path(&self, nid: u64) -> PathBuf {
        self.metas_path().join(nid.to_string())
    }

    /// Return the path of the directory containing the inodes.
    pub(super) fn metas_path(&self) -> PathBuf {
        self.fs_path.join("meta")
    }

    pub(super) fn dir_path(&self, nid: u64) -> PathBuf {
        self.dirs_path().join(nid.to_string())
    }

    pub(super) fn dirs_path(&self) -> PathBuf {
        self.fs_path.join("dir")
    }

    pub(super) fn file_path(&self, nid: u64) -> PathBuf {
        self.files_path().join(nid.to_string())
    }

    pub(super) fn files_path(&self) -> PathBuf {
        self.fs_path.join("file")
    }

    /// Return the path of the superblock file.
    pub(super) fn superblock_path(&self) -> PathBuf {
        self.fs_path.join("superblock")
    }

    pub(super) fn is_direct(&self) -> bool {
        self.is_direct
    }
}

impl FuseEntry {
    pub fn new(ttl: Duration, attr: FileAttr, generation: u64) -> Self {
        Self {
            ttl,
            attr,
            generation,
        }
    }
}

impl FuseAttr {
    pub fn new(ttl: Duration, attr: FileAttr) -> Self {
        Self { ttl, attr }
    }
}

impl SyncFs {
    /// Create a new `SyncFs` instance. No concurrency involved.
    pub fn new(config: SyncFsConfig) -> Self {
        let fs = Self {
            config: config.clone(),
            nnid: Mutex::new(2), // at least be 2, because 1 if preserved for root dir
            next_file_handle: AtomicUsize::new(0),
            mbp: BufferPool::new(config.clone()),
            dbp: BufferPool::new(config.clone()),
            lp: RwLock::new(HashMap::new()),
        };

        fs
    }

    /// Allocate a new inode id.
    ///
    /// Currently, because there isn't anything that should be stateful,,  
    pub(super) fn alloc_node_id(&self) -> u64 {
        let mut nnid_guard = self.nnid.lock().unwrap();
        let alloc = *nnid_guard;
        *nnid_guard += 1;

        // durability
        fs::write(self.config.superblock_path(), (alloc + 1).to_ne_bytes()).unwrap();

        alloc
    }
}

impl SyncFs {
    fn init_impl(
        &self,
        _req: &fuser::Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), c_int> {
        fs::create_dir_all(self.config.metas_path()).unwrap();
        fs::create_dir_all(self.config.dirs_path()).unwrap();
        fs::create_dir_all(self.config.files_path()).unwrap();

        // init next node id from possible superblock
        let sbp = self.config.superblock_path();
        let nid = fs::read(&sbp);
        if let Ok(iid) = nid {
            // load iid from existing superblock
            *self.nnid.lock().unwrap() = bincode::deserialize(&iid).unwrap();
        } else {
            // superblock is not existing
            let nnid_guard = self.nnid.lock().unwrap();
            fs::write(&sbp, bincode::serialize(&(*nnid_guard)).unwrap()).unwrap();
        }

        // create the root dir representation if there is none
        if fs::metadata(self.config.meta_path(ROOT_DIR_NID)).is_err() {
            // setup metadata
            let mhandle = self.mbp.create(&ROOT_DIR_NID)?;
            let mut meta = mhandle.data_mut();
            let now = SystemTime::now();
            meta.create(now, FileTy::Dir, 0o777, 0, 0);

            // setup directory
            let dhandle = self.dbp.create(&ROOT_DIR_NID)?;
            let mut dents = dhandle.data_mut();
            dents.insert(".".to_string(), ROOT_DIR_NID, FileTy::Dir);
            dents.insert("..".to_string(), ROOT_DIR_NID, FileTy::Dir);
        }

        Ok(())
    }

    fn lookup_impl(
        &self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
    ) -> Result<FuseEntry, c_int> {
        debug!("[entry] enter lookup");
        let dhandle = self.dbp.fetch(&parent)?;
        let dents = dhandle.data();
        if let Ok((nid, _)) = dents.get(name.to_str().unwrap()) {
            let mhandle = self.mbp.fetch(nid)?;
            let mut metadata = mhandle.data_mut();
            metadata.atime = SystemTime::now();
            Ok(FuseEntry::new(Duration::new(0, 0), metadata.into_attr(), 0))
        } else {
            warn!("[dir] cannot find entry {:?} in {}", name, parent);
            Err(libc::ENOENT)
        }
    }

    fn mknod_impl(
        &self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
    ) -> Result<FuseEntry, c_int> {
        debug!("[entry] enter mknod");
        // check file type, because only a handful of functions are implemented
        let file_ty = mode & libc::S_IFMT;
        if file_ty != libc::S_IFREG {
            return Err(if file_ty == libc::S_IFDIR {
                libc::EINVAL
            } else {
                error!("try to create a file with unsupported type {:o}", file_ty);
                libc::ENOSYS
            });
        }

        // check if it's existing
        let pdhandle = self.dbp.fetch(&parent)?;
        let mut pdents = pdhandle.data_mut();
        let name = name.to_str().unwrap().to_string();
        if pdents.contains_key(&name) {
            return Err(libc::EEXIST);
        }

        let pmhandle = self.mbp.fetch(&parent)?;
        let mut pmeta = pmhandle.data_mut();

        let nid = self.alloc_node_id();

        // modify parent's metadata
        let now = SystemTime::now();
        pmeta.modify(now);

        // modify parent's entries, currently only file is supported
        pdents.insert(name, nid, FileTy::File);

        // write metadata
        let file_ty = FileTy::from_mode(mode);
        let mhandle = self.mbp.create(&nid)?;
        let mut meta = mhandle.data_mut();
        // todo: mode is different when supporting suid, and the setting of gid should consider parent?
        meta.create(now, file_ty, mode as u16, req.uid(), req.gid());

        // write content
        let dpath = self.config.file_path(nid);
        fs::write(&dpath, &[]).unwrap();

        Ok(FuseEntry::new(Duration::new(0, 0), meta.into_attr(), 0))
    }

    fn mkdir_impl(
        &self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
    ) -> Result<FuseEntry, c_int> {
        debug!("[entry] enter mkdir");
        // check if it's existing
        let pdhandle = self.dbp.fetch(&parent)?;
        let mut pdents = pdhandle.data_mut();
        let name = name.to_str().unwrap().to_string();
        if pdents.contains_key(&name) {
            return Err(libc::EEXIST);
        }

        let pmhandle = self.mbp.fetch(&parent)?;
        let mut pmeta = pmhandle.data_mut();

        let nid = self.alloc_node_id();

        // modify parent's metadata
        let now = SystemTime::now();
        pmeta.modify(now);

        // modify parent's entries
        pdents.insert(name, nid, FileTy::Dir);

        // write metadata
        let mhandle = self.mbp.create(&nid)?;
        let mut meta = mhandle.data_mut();
        // todo: mode is different when supporting suid, and the setting of gid should consider parent?
        meta.create(now, FileTy::Dir, mode as u16, req.uid(), req.gid());

        // write content
        let dhandle = self.dbp.create(&nid)?;
        let mut dents = dhandle.data_mut();
        dents.insert(".".to_string(), nid, FileTy::Dir);
        dents.insert("..".to_string(), parent, FileTy::Dir);

        Ok(FuseEntry::new(Duration::new(0, 0), meta.into_attr(), 0))
    }

    fn rmdir_impl(
        &self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
    ) -> Result<(), c_int> {
        debug!("[entry] enter rmdir");
        let name = name.to_str().unwrap().to_string();

        // check if it's existing
        let pdhandle = self.dbp.fetch(&parent)?;
        let mut pdents = pdhandle.data_mut();
        if let Some(_) = pdents.remove(&name) {
            Ok(())
        } else {
            warn!("[dir] cannot find entry {:?} in {} to remove", name, parent);
            Err(libc::ENOENT)
        }
    }

    fn rename_impl(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
    ) -> Result<(), c_int> {
        debug!("[entry] enter rename");
        let now = SystemTime::now();
        let name = name.to_str().unwrap().to_string();
        let newname = newname.to_str().unwrap().to_string();

        if parent == newparent {
            let pmhandle = self.mbp.fetch(&parent)?;
            let mut pmeta = pmhandle.data_mut();
            let pdhandle = self.dbp.fetch(&parent)?;
            let mut pdents = pdhandle.data_mut();

            // exchange
            #[cfg(target_os = "linux")]
            if flags & libc::RENAME_EXCHANGE as u32 != 0 {
                let (nid, ty) = *pdents.get(&name)?;
                let (nnid, nty) = *pdents.get(&newname)?;

                if name == newname {
                    return Ok(());
                }

                // change metadata
                pmeta.modify(now);

                // change entries
                pdents.insert(name, nnid, nty);
                pdents.insert(newname, nid, ty);
                return Ok(());
            }

            // rename
            let (nid, ty) = *pdents.get(&name)?;
            let res = pdents.get(&name);
            if let Ok(&(nnid, nty)) = res {
                let nmhandle = self.mbp.fetch(&nnid)?;
                let mut nmeta = nmhandle.data_mut();
                if nmeta.ty == FileTy::Dir {
                    let ndhandle = self.dbp.fetch(&nnid)?;
                    let ndents = ndhandle.data();
                    if ndents.len() > 2 {
                        return Err(libc::ENOTEMPTY);
                    }
                }
                let mhandle = self.mbp.fetch(&nid)?;
                let meta = mhandle.data();
                if nmeta.ty == meta.ty {
                    // change metadata
                    pmeta.modify(now);
                    nmeta.unlink(&self.config);

                    // change entries
                    pdents.insert(newname, nid, ty);
                    pdents.remove(&name);

                    return Ok(());
                } else if nmeta.ty == FileTy::Dir {
                    return Err(libc::EISDIR);
                } else {
                    return Err(libc::ENOTDIR);
                }
            } else {
                // change metadata
                pmeta.modify(now);

                // change entries
                pdents.insert(newname, nid, ty);
                pdents.remove(&name);

                return Ok(());
            }
        } else {
            // check perm
            let pmhandle = self.mbp.fetch(&parent)?;
            let mut pmeta = pmhandle.data_mut();
            let npmhandle = self.mbp.fetch(&newparent)?;
            let mut npmeta = npmhandle.data_mut();

            let pdhandle = self.dbp.fetch(&parent)?;
            let mut pdents = pdhandle.data_mut();
            let npdhandle = self.dbp.fetch(&newparent)?;
            let mut npdents = npdhandle.data_mut();

            // exchange
            #[cfg(target_os = "linux")]
            if flags & libc::RENAME_EXCHANGE as u32 != 0 {
                let (nid, ty) = *pdents.get(&name)?;
                let (nnid, nty) = *npdents.get(&newname)?;

                if name == newname {
                    return Ok(());
                }

                // change metadata
                pmeta.modify(now);

                // change entries
                pdents.insert(name, nnid, nty);
                pdents.insert(newname, nid, ty);

                if ty == FileTy::Dir {
                    let dhandle = self.dbp.fetch(&nid)?;
                    let mut dents = dhandle.data_mut();
                    dents.insert("..".to_string(), newparent, FileTy::Dir);
                }
                if nty == FileTy::Dir {
                    let ndhandle = self.dbp.fetch(&nnid)?;
                    let mut ndents = ndhandle.data_mut();
                    ndents.insert("..".to_string(), parent, FileTy::Dir);
                }

                return Ok(());
            }

            // rename
            let (nid, ty) = *pdents.get(&name)?;
            let res = npdents.get(&name);
            if let Ok(&(nnid, nty)) = res {
                if nty == FileTy::Dir {
                    let ndhandle = self.dbp.fetch(&nnid)?;
                    let ndents = ndhandle.data();
                    if ndents.len() > 2 {
                        return Err(libc::ENOTEMPTY);
                    }
                }
                if nty == ty {
                    let nmhandle = self.mbp.fetch(&nnid)?;
                    let mut nmeta = nmhandle.data_mut();
                    let mhandle = self.mbp.fetch(&nid)?;
                    let meta = mhandle.data();

                    // change metadata
                    pmeta.modify(now);
                    nmeta.unlink(&self.config);

                    // change entries
                    pdents.insert(newname, nid, ty);
                    pdents.remove(&name);

                    if nmeta.ty == FileTy::Dir {
                        let dhandle = self.dbp.fetch(&nid)?;
                        let mut dents = dhandle.data_mut();
                        dents.insert("..".to_string(), newparent, FileTy::Dir);

                        let ndhandle = self.dbp.fetch(&nnid)?;
                        let mut ndents = ndhandle.data_mut();
                        ndents.insert("..".to_string(), parent, FileTy::Dir);
                    }

                    return Ok(());
                } else if nty == FileTy::Dir {
                    return Err(libc::EISDIR);
                } else {
                    return Err(libc::ENOTDIR);
                }
            } else {
                // change metadata
                pmeta.modify(now);

                // change entries
                pdents.insert(newname, nid, ty);
                pdents.remove(&name);

                return Ok(());
            }
        }
    }

    fn getattr_impl(&mut self, req: &fuser::Request<'_>, ino: u64) -> Result<FuseAttr, c_int> {
        debug!("[entry] enter getattr");
        let mhandle = self.mbp.fetch(&ino)?;
        let meta = mhandle.data();
        Ok(FuseAttr::new(Duration::new(0, 0), meta.into_attr()))
    }

    /// All parameters that are unused is unimplemented.
    fn setattr_impl(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
    ) -> Result<FuseAttr, c_int> {
        let mhandle = self.mbp.fetch(&ino)?;
        let mut meta = mhandle.data_mut();
        let now = SystemTime::now();
        meta.change(now);

        if let Some(size) = size {
            // modify inode
            meta.size = size;

            // modify data
            let dpath = self.config.file_path(ino);
            let file = OpenOptions::new()
                .write(true)
                .open(&dpath)
                .map_err(|_| libc::EIO)?;
            file.set_len(size).map_err(|_| libc::EIO)?;
        }

        if let Some(atime) = atime {
            match atime {
                fuser::TimeOrNow::SpecificTime(atime) => meta.atime = atime,
                fuser::TimeOrNow::Now => meta.atime = SystemTime::now(),
            }
        }

        if let Some(mtime) = mtime {
            match mtime {
                fuser::TimeOrNow::SpecificTime(mtime) => meta.mtime = mtime,
                fuser::TimeOrNow::Now => meta.mtime = SystemTime::now(),
            }
        }

        if let Some(ctime) = ctime {
            meta.ctime = ctime;
        }

        if let Some(crtime) = crtime {
            meta.crtime = crtime;
        }

        Ok(FuseAttr::new(Duration::new(0, 0), meta.into_attr()))
    }

    fn opendir_impl(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        flags: i32,
    ) -> Result<(u64, u32), c_int> {
        let (readable, writable) = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => {
                if flags & libc::O_TRUNC != 0 {
                    return Err(libc::EISDIR);
                }
                (true, false)
            }
            libc::O_WRONLY => (false, true),
            libc::O_RDWR => (true, true),
            _ => {
                return Err(libc::EINVAL);
            }
        };

        // todo: I think I should check the permission here
        let mhandle = self.mbp.fetch(&ino)?;
        let mut meta = mhandle.data_mut();
        meta.open();

        Ok((self.alloc_node_id(), 0))
    }

    fn readdir_impl(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
    ) -> Result<FuseDir, c_int> {
        debug!("[entry] enter readdir");
        let mhandle = self.mbp.fetch(&ino)?;
        let mut meta = mhandle.data_mut();

        // modify metadata
        let now = SystemTime::now();
        meta.access(now);

        let dhandle = self.dbp.fetch(&ino)?;
        let dents = dhandle.data();
        Ok(dents
            .iter()
            .skip(offset as usize)
            .enumerate()
            .map(|(i, (str, (nid, ty)))| (*nid, offset + i as i64 + 1, ty.clone(), str.clone()))
            .collect())
    }

    fn releasedir_impl(&mut self, ino: u64) -> Result<(), c_int> {
        let mhandle = self.mbp.fetch(&ino)?;
        let mut meta = mhandle.data_mut();
        meta.close(&self.config);
        Ok(())
    }

    fn open_impl(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        flags: i32,
    ) -> Result<(u64, u32), c_int> {
        let (readable, writable) = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => {
                if flags & libc::O_TRUNC != 0 {
                    return Err(libc::EISDIR);
                }
                (true, false)
            }
            libc::O_WRONLY => (false, true),
            libc::O_RDWR => (true, true),
            _ => {
                return Err(libc::EINVAL);
            }
        };

        // todo: I think I should check the permission here
        let mhandle = self.mbp.fetch(&ino)?;
        let mut meta = mhandle.data_mut();
        meta.open();

        Ok((self.alloc_node_id(), 0))
    }

    /// Lock owner is currently unimplemented.
    fn read_impl(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
    ) -> Result<Vec<u8>, c_int> {
        let dpath = self.config.file_path(ino);

        // metadata modification
        {
            let mhandle = self.mbp.fetch(&ino)?;
            let mut meta = mhandle.data_mut();
            meta.access(SystemTime::now());
        }

        let mut lp_guard = self.lp.read().unwrap();
        let latch = if let Some(latch) = lp_guard.get(&ino) {
            latch
        } else {
            drop(lp_guard);

            let mut lp_wguard = self.lp.write().unwrap();
            lp_wguard.insert(ino, RwLock::new(()));
            drop(lp_wguard);

            lp_guard = self.lp.read().unwrap();
            lp_guard.get(&ino).unwrap()
        };

        let _guard = latch.read().unwrap();
        if let Ok(file) = File::open(dpath) {
            let mut reader = BufReader::new(file);
            reader.seek(SeekFrom::Start(offset as u64)).unwrap();
            let mut buf = vec![0; size as usize];
            let len = reader.read(&mut buf).unwrap();
            buf.truncate(len);
            Ok(buf)
        } else {
            Err(libc::EIO)
        }
    }

    fn write_impl(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        write_flags: u32,
        flags: i32,
        lock_owner: Option<u64>,
    ) -> Result<u32, c_int> {
        let dpath = self.config.file_path(ino);

        // metadata modification
        {
            let mhandle = self.mbp.fetch(&ino)?;
            let mut meta = mhandle.data_mut();
            meta.modify(SystemTime::now());
        }

        let mut lp_guard = self.lp.read().unwrap();
        let latch = if let Some(latch) = lp_guard.get(&ino) {
            latch
        } else {
            drop(lp_guard);

            let mut lp_wguard = self.lp.write().unwrap();
            lp_wguard.insert(ino, RwLock::new(()));
            drop(lp_wguard);

            lp_guard = self.lp.read().unwrap();
            lp_guard.get(&ino).unwrap()
        };

        let _guard = latch.write().unwrap();
        if let Ok(file) = File::create(&dpath) {
            let mut writer = BufWriter::new(file);
            writer.seek(SeekFrom::Start(offset as u64)).unwrap();
            let len = writer.write(data).unwrap();
            writer.flush().unwrap();

            // modify size
            self.mbp.fetch(&ino)?.data_mut().size = fs::metadata(dpath).unwrap().len();

            Ok(len as u32)
        } else {
            warn!("[file] fail to write {:?}", dpath);
            Err(libc::EIO)
        }
    }

    fn release_impl(&mut self, ino: u64) -> Result<(), c_int> {
        let mhandle = self.mbp.fetch(&ino)?;
        let mut meta = mhandle.data_mut();
        meta.close(&self.config);
        Ok(())
    }
}

impl Filesystem for SyncFs {
    fn init(
        &mut self,
        req: &fuser::Request<'_>,
        config: &mut fuser::KernelConfig,
    ) -> Result<(), c_int> {
        self.init_impl(req, config)
    }

    fn lookup(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: fuser::ReplyEntry,
    ) {
        match self.lookup_impl(req, parent, name) {
            Ok(entry) => reply.entry(&Duration::new(0, 0), &entry.attr, entry.generation),
            Err(err) => reply.error(err),
        }
    }

    fn mknod(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: fuser::ReplyEntry,
    ) {
        match self.mknod_impl(req, parent, name, mode, umask, rdev) {
            Ok(entry) => reply.entry(&Duration::new(0, 0), &entry.attr, entry.generation),
            Err(err) => reply.error(err),
        }
    }

    fn mkdir(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        match self.mkdir_impl(req, parent, name, mode, umask) {
            Ok(entry) => reply.entry(&Duration::new(0, 0), &entry.attr, entry.generation),
            Err(err) => reply.error(err),
        }
    }

    fn rmdir(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        match self.rmdir_impl(req, parent, name) {
            Ok(_) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn rename(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        match self.rename_impl(req, parent, name, newparent, newname, flags) {
            Ok(_) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn getattr(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        match self.getattr_impl(_req, ino) {
            Ok(attr) => reply.attr(&Duration::new(0, 0), &attr.attr),
            Err(err) => reply.error(err),
        }
    }

    fn setattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: fuser::ReplyAttr,
    ) {
        match self.setattr_impl(
            _req, ino, mode, uid, gid, size, atime, mtime, ctime, fh, crtime, _chgtime, _bkuptime,
            flags,
        ) {
            Ok(attr) => reply.attr(&Duration::new(0, 0), &attr.attr),
            Err(err) => reply.error(err),
        }
    }

    fn opendir(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        _flags: i32,
        reply: fuser::ReplyOpen,
    ) {
        match self.opendir_impl(_req, _ino, _flags) {
            Ok((fh, flags)) => reply.opened(fh, flags),
            Err(err) => reply.error(err),
        }
    }

    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        match self.readdir_impl(_req, ino, fh, offset) {
            Ok(dir) => {
                for (i, offset, ty, name) in dir {
                    // whether the buffer is full
                    if reply.add(i, offset, ty.into(), &name) {
                        break;
                    }
                }
                reply.ok();
            }
            Err(err) => reply.error(err),
        }
    }

    fn releasedir(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        flags: i32,
        reply: fuser::ReplyEmpty,
    ) {
        match self.releasedir_impl(ino) {
            Ok(_) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn open(&mut self, _req: &fuser::Request<'_>, _ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        match self.open_impl(_req, _ino, _flags) {
            Ok((fh, flags)) => reply.opened(fh, flags),
            Err(err) => reply.error(err),
        }
    }

    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        match self.read_impl(_req, ino, fh, offset, size, flags, lock_owner) {
            Ok(data) => reply.data(&data),
            Err(err) => reply.error(err),
        }
    }

    fn write(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        write_flags: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        match self.write_impl(req, ino, fh, offset, data, write_flags, flags, lock_owner) {
            Ok(len) => reply.written(len),
            Err(err) => reply.error(err),
        }
    }

    fn release(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        match self.release_impl(ino) {
            Ok(_) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }
}
