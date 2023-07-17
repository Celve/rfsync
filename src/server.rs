use std::{
    collections::HashMap,
    fmt::Debug,
    mem::MaybeUninit,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Weak,
    },
};

use async_recursion::async_recursion;
use tokio::{
    sync::{Mutex, RwLock},
    task::JoinHandle,
};

use crate::{
    listener::Listener,
    path::{RelPath, RootPath},
    peer::Peer,
    sync::SyncCell,
    watcher::Watcher,
};

pub struct Server {
    pub addr: SocketAddr,
    pub root: RootPath,
    map: RwLock<HashMap<RelPath, Weak<SyncCell>>>,
    placeholder: Mutex<MaybeUninit<Arc<SyncCell>>>,

    /// Other servers.
    pub peers: RwLock<Vec<Peer>>,

    /// The `time - 1` represents the last time the server is updated.
    pub time: AtomicUsize,

    /// The unique synchronization id.
    pub sid: AtomicUsize,

    pub id: usize,
}

impl Server {
    pub async fn new(addr: SocketAddr, path: &RootPath, id: usize) -> Arc<Self> {
        let server = Arc::new(Self {
            addr,
            root: path.clone().into(),
            map: RwLock::new(HashMap::new()),
            placeholder: Mutex::new(MaybeUninit::uninit()),
            peers: RwLock::new(Vec::new()),
            time: AtomicUsize::new(0),
            sid: AtomicUsize::new(0),
            id,
        });

        // put scinto place holder without initing
        let sc = SyncCell::empty(&server, &RelPath::default()).await;
        server.placeholder.lock().await.write(sc.clone());
        server.add_sc(&sc).await;

        server
    }

    /// Activate the server.
    pub fn run(self: &Arc<Self>) -> Vec<JoinHandle<()>> {
        tokio::spawn(self.clone().init());

        let srv = Arc::downgrade(self);
        let listener = Listener::new(self.addr, srv.clone());
        let listen_handle = tokio::spawn(listener.listen());

        let src = self.root.clone();
        let tmp = self.tmp_path();
        let mut watcher = Watcher::new(src, tmp, srv);
        watcher.init();
        let watch_handle = tokio::task::spawn_blocking(move || watcher.watch());

        vec![listen_handle, watch_handle]
    }
}

// interface for local modification
impl Server {
    pub async fn init(self: Arc<Self>) {
        let sc = self.clone().make_sc(&RelPath::default()).await;
        sc.clone()
            .create(self.time.fetch_add(1, Ordering::AcqRel))
            .await;
        sc.broadcast().await;
    }

    pub async fn create(self: Arc<Self>, rel: RelPath) {
        let sc = self.clone().make_sc(&rel).await;
        sc.clone()
            .create(self.time.fetch_add(1, Ordering::AcqRel))
            .await;
        sc.clone().sendup_meta().await;
        sc.clone().broadcast().await;
    }

    pub async fn remove(self: Arc<Self>, rel: RelPath) {
        let sc = self.clone().make_sc(&rel).await;
        sc.clone()
            .remove(self.time.fetch_add(1, Ordering::AcqRel))
            .await;
        sc.clone().sendup_meta().await;
        sc.clone().broadcast().await;
    }

    pub async fn modify(self: Arc<Self>, rel: RelPath) {
        let sc = self.clone().make_sc(&rel).await;
        sc.clone()
            .modify(self.time.fetch_add(1, Ordering::AcqRel))
            .await;
        sc.clone().sendup_meta().await;
        sc.clone().broadcast().await;
    }
}

impl Server {
    /// Get the `SyncCell` from the server. Return `None` when there is none.
    pub async fn get_sc(&self, path: &RelPath) -> Option<Arc<SyncCell>> {
        self.map
            .read()
            .await
            .get(path)
            .map(|cell| cell.upgrade().unwrap().clone())
    }

    /// Get the `SyncCell` from the server if existed.
    /// Otherwise, create a new one with the given path.
    /// All missing parent would be created likewise.
    #[async_recursion]
    pub async fn make_sc(self: Arc<Self>, path: &RelPath) -> Arc<SyncCell> {
        let sc = self.get_sc(path).await;
        if let Some(sc) = sc {
            sc
        } else {
            // make sure again, because the lock is released
            let parent = self.clone().make_sc(&path.parent()).await;
            let mut parent_guard = parent.lock().await;
            if parent_guard.children.contains_key(&path) {
                parent_guard.children.get(&path).unwrap().clone()
            } else {
                let sc = SyncCell::empty(&self, path).await;
                parent_guard.children.insert(path.clone(), sc.clone());
                sc
            }
        }
    }

    /// Add the `SyncCell` to the server `HashMap`.
    pub async fn add_sc(&self, cell: &Arc<SyncCell>) {
        self.map
            .write()
            .await
            .insert(cell.rel.clone(), Arc::downgrade(cell));
    }
}

impl Server {
    pub fn tmp_path(&self) -> RootPath {
        let mut path = home::home_dir().unwrap();
        path.push(".rfsync");
        path.push(self.id.to_string());
        RootPath::new(path)
    }
}

impl Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("id", &self.id)
            .field("time", &self.time)
            .finish()
    }
}
