use std::{net::SocketAddr, sync::Arc};

use async_recursion::async_recursion;
use futures_util::future::join_all;
use tokio::fs;
use tracing::{error, info, instrument};

use super::{copy::CopyCell, remote::RemoteCell, CellType, TraCell, TraCellInner};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SyncOp {
    Copy,
    Conflict,
    Recurse,
    None,
}

impl TraCell {
    #[instrument]
    pub async fn sync(self: Arc<Self>, addr: SocketAddr) {
        info!("begin to sync");
        let sid = self
            .server
            .sid
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut path = self.server.tmp_path().as_path_buf();
        path.push(sid.to_string());
        fs::create_dir_all(path).await.unwrap();
        let rc = RemoteCell::from_path(addr, self.rel.clone()).await;
        let cc = CopyCell::make(sid, self.clone(), rc, self.rel.parent()).await;
        info!("copy done");
        self.clone().sync_copy(sid, cc, addr).await;
        info!("sync done");

        // begin to remove cache
        let path = self.server.tmp_path().as_path_buf().join(sid.to_string());
        let meta = fs::metadata(&path).await;
        if let Ok(meta) = meta {
            if meta.is_dir() {
                fs::remove_dir_all(&path).await.unwrap();
            } else {
                fs::remove_file(&path).await.unwrap();
            }
        }
    }

    #[instrument]
    #[async_recursion]
    pub async fn sync_copy(self: Arc<Self>, sid: usize, cc: CopyCell, addr: SocketAddr) {
        if cc.op != SyncOp::None {
            let mut self_guard = self.lock().await;
            if self_guard.ts == cc.ts {
                match cc.op {
                    SyncOp::Copy => {
                        info!("rename {:?} -> {:?}", cc.path(), self.path());
                        if let Err(e) = fs::rename(cc.path(), self.path().as_path_buf()).await {
                            error!("{:?} with {:?} and {:?}", e, cc.path(), self.path());
                        }
                        self_guard.reshape_from_cc(&cc);
                    }

                    SyncOp::Conflict => {
                        let mut bak = self.path().as_path_buf();
                        bak.push(".bak");
                        info!("rename {:?} -> {:?}", cc.path(), bak);
                        if let Err(e) = fs::rename(cc.path(), bak).await {
                            error!("{:?}", e);
                        }
                        self_guard.reshape_from_cc(&cc);
                    }

                    SyncOp::Recurse => {
                        info!("recurse");
                        let mut handles = Vec::new();
                        for cc in cc.children.iter() {
                            // create a tc if there is none
                            let tc = if self_guard.children.contains_key(&cc.offset) {
                                self_guard.children.get(&cc.offset).unwrap().clone()
                            } else {
                                let tc = Self::empty(&self.server, &cc.rel).await;
                                self_guard.children.insert(cc.offset.clone(), tc.clone());
                                tc
                            };
                            handles.push(tokio::spawn(tc.sync_copy(sid, cc.clone(), addr)));
                        }
                        join_all(handles).await;
                    }

                    SyncOp::None => {
                        info!("none");
                        // do nothing
                    }
                }
            } else {
                info!("redo");
                // we need to redo the sync
                let rc = RemoteCell::from_path(addr, self.rel.clone()).await;
                let cc = CopyCell::make(sid, self.clone(), rc, self.rel.clone()).await;
                self.clone().sync_copy(sid, cc, addr).await;
            }
        }
    }
}

impl TraCellInner {
    pub fn reshape_from_cc(&mut self, cc: &CopyCell) {
        self.modif = cc.modif.clone();
        self.sync = cc.sync.clone();
        self.crt = cc.crt;
        self.ty = cc.ty;
    }
}

impl TraCellInner {
    /// Synchronize the file with another server.
    #[instrument]
    pub(super) fn get_sop(&self, rc: &RemoteCell) -> SyncOp {
        match (self.ty, rc.ty) {
            (CellType::Dir, CellType::Dir) => self.get_sdop(rc),
            _ => self.get_sfop(rc),
        }
    }

    /// Do the sync job when both the src and dst are dirs.
    #[instrument]
    fn get_sdop(&self, rc: &RemoteCell) -> SyncOp {
        if self.is_existing() || rc.is_existing() {
            if rc.modif <= self.sync {
                SyncOp::None
            } else {
                SyncOp::Recurse
            }
        } else {
            SyncOp::None
        }
    }

    /// Do the sync job when at least one of the src or the dst is not dir.
    #[instrument]
    fn get_sfop(&self, rc: &RemoteCell) -> SyncOp {
        if !self.is_existing() && rc.is_existing() {
            if rc.modif <= self.sync {
                SyncOp::None
            } else if !(rc.crt <= self.sync) {
                SyncOp::Copy
            } else {
                SyncOp::Conflict
            }
        } else if self.is_existing() || rc.is_existing() {
            if rc.modif <= self.sync {
                SyncOp::None
            } else if self.modif <= rc.sync {
                SyncOp::Copy
            } else {
                SyncOp::Conflict
            }
        } else {
            SyncOp::None
        }
    }
}
