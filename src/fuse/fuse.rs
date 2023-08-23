use std::{
    ffi::OsStr,
    sync::Arc,
    time::{Duration, SystemTime},
};

use fuser::{
    Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen,
    ReplyWrite, Request, TimeOrNow,
};
use tokio::runtime::Runtime;

use super::server::SyncServer;

pub struct SyncFuse<const S: usize> {
    srv: SyncServer<S>,
    rt: Arc<Runtime>,
}

impl<const S: usize> SyncFuse<S> {
    pub fn new(rt: Arc<Runtime>, srv: SyncServer<S>) -> Self {
        Self { srv, rt }
    }

    fn osstr2str(osstr: &OsStr) -> &str {
        osstr.to_str().unwrap()
    }

    fn time2time(t: Option<TimeOrNow>, now: SystemTime) -> Option<SystemTime> {
        if let Some(t) = t {
            Some(match t {
                TimeOrNow::SpecificTime(t) => t,
                TimeOrNow::Now => now,
            })
        } else {
            None
        }
    }
}

impl<const S: usize> Filesystem for SyncFuse<S> {
    fn mknod(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        match self.rt.block_on(self.srv.mknod(
            &parent,
            Self::osstr2str(name),
            mode as u16,
            req.uid(),
            req.gid(),
        )) {
            Ok(meta) => reply.entry(&Duration::new(0, 0), &meta.into(), 0),

            Err(e) => reply.error(e),
        }
    }

    fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        match self.rt.block_on(self.srv.mkdir(
            &parent,
            Self::osstr2str(name),
            mode as u16,
            req.uid(),
            req.gid(),
        )) {
            Ok(meta) => reply.entry(&Duration::new(0, 0), &meta.into(), 0),
            Err(e) => reply.error(e),
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self
            .rt
            .block_on(self.srv.unlink(&parent, Self::osstr2str(name)))
        {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self
            .rt
            .block_on(self.srv.rmdir(&parent, Self::osstr2str(name)))
        {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        match self.rt.block_on(self.srv.open(&ino)) {
            Ok(fh) => reply.opened(fh, 0),
            Err(e) => reply.error(e),
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        match self.rt.block_on(self.srv.read(&ino, &offset, &size)) {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(e),
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        write_flags: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        match self.rt.block_on(self.srv.write(&ino, &offset, &data)) {
            Ok(size) => reply.written(size as u32),
            Err(e) => reply.error(e),
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        match self.rt.block_on(self.srv.release(&ino)) {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        match self.rt.block_on(self.srv.getattr(&ino)) {
            Ok(meta) => reply.attr(&Duration::new(0, 0), &meta.into()),
            Err(e) => reply.error(e),
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        match self.rt.block_on(async {
            let now = SystemTime::now();
            let atime = Self::time2time(atime, now);
            let mtime = Self::time2time(mtime, now);

            self.srv
                .setattr(&ino, size, atime, mtime, ctime, crtime)
                .await
        }) {
            Ok(meta) => reply.attr(&Duration::new(0, 0), &meta.into()),
            Err(e) => reply.error(e),
        }
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match self
            .rt
            .block_on(self.srv.lookup(&parent, Self::osstr2str(name)))
        {
            Ok(meta) => reply.entry(&Duration::new(0, 0), &meta.into(), 0),
            Err(e) => reply.error(e),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        match self.rt.block_on(self.srv.readdir(&ino, offset)) {
            Ok(dents) => {
                for (ino, offset, ty, name) in dents {
                    if reply.add(ino, offset, ty.into(), name) {
                        break;
                    }
                }
                reply.ok();
            }
            Err(_) => todo!(),
        }
    }
}
