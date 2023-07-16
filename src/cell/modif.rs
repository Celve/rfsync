use std::{ops::Sub, sync::Arc};

use async_recursion::async_recursion;
use futures_util::future::join_all;
use tokio::fs;
use tracing::{info, instrument};

use crate::{
    comm::{Comm, Request, Response},
    path::AbsPath,
    server::Server,
};

use super::{CellType, TraCell, TraCellInner};

// modification functions
impl TraCell {
    /// Create a new file or directory. Increase the server time while creating.
    ///
    /// If it's a directory, it would recurse down, adding all of its component to the server.
    #[instrument]
    #[async_recursion]
    pub async fn create(self: Arc<Self>, time: usize) {
        let mut self_guard = self.lock().await;
        let path = self.path().as_path_buf();

        // update modif vectime
        if !self_guard.modify(&self.server, time) {
            return;
        }
        if let Ok(metadata) = fs::metadata(&path).await {
            if metadata.is_dir() {
                if self_guard.ty != CellType::Dir {
                    self_guard.ty = CellType::Dir;
                }

                // recurse down
                let dir = fs::read_dir(&path).await;
                let mut handles = Vec::new();
                if let Ok(mut stream) = dir {
                    while let Some(entry) = stream.next_entry().await.unwrap() {
                        let path = AbsPath::new(entry.path()).sub(&self.server.root).unwrap();
                        let child = if let Some(child) = self_guard.get_child(&path) {
                            child
                        } else {
                            let cell = Self::empty(&self.server, &path).await;
                            self_guard.add_child(cell.clone());

                            cell
                        };
                        handles.push(tokio::spawn(child.create(time)));
                    }
                } else {
                    // maybe some consistency conflicts
                }

                // wait for all children to finish
                info!("create {:?}", self_guard);
                drop(self_guard);
                join_all(handles).await;
                self.lock().await.sum_children().await;
            } else {
                // file
                self_guard.ty = CellType::File;
                info!("create {:?}", self_guard);
                drop(self_guard);
            }
        } else {
            drop(self_guard);
            // there might be some other file system changes
        }
    }

    #[instrument]
    #[async_recursion]
    pub async fn remove(self: Arc<Self>, time: usize) {
        let handles = {
            let mut self_guard = self.lock().await;

            // update modif vectime
            if !self_guard.modify(&self.server, time) {
                return;
            }

            self_guard.ty = CellType::None;
            let mut handles = Vec::new();
            for child in self_guard.children.values() {
                let child = child.clone();
                if child.lock().await.ty == CellType::Dir {
                    handles.push(tokio::spawn(child.remove(time)));
                }
            }
            handles
        };
        join_all(handles).await;
        self.lock().await.sum_children().await;
    }

    #[instrument]
    pub async fn modify(self: Arc<Self>, time: usize) {
        let mut self_guard = self.lock().await;
        self_guard.modify(&self.server, time);
    }

    #[instrument]
    #[async_recursion]
    pub async fn broadcast(self: Arc<Self>) {
        info!("broadcast");
        let server = self.server.clone();
        for peer in server.peers.read().await.iter() {
            if peer.id == server.id {
                continue;
            }

            let req = Request::SyncCell(server.as_ref().into(), self.rel.clone());
            info!("send {:?}", req);
            let res = Comm::new(peer.addr).request(&req).await;

            match res {
                Response::Sync => {
                    // do nothing
                }
                _ => panic!("unexpected response"),
            }
        }
    }
}

impl TraCellInner {
    pub fn modify(&mut self, server: &Arc<Server>, time: usize) -> bool {
        let id = server.id;
        if self.ts <= time {
            if self.modif.is_empty() {
                self.crt = time;
            }
            self.modif.insert(id, time);
            self.ts = time;
            true
        } else {
            false
        }
    }
}
