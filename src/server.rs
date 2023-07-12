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
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{Mutex, RwLock},
    task::JoinHandle,
};
use tracing::{info, instrument};

use crate::{
    cell::TraCell,
    comm::{Request, Response},
    path::{RelPath, RootPath},
    peer::Peer,
    watcher::Watcher,
};

pub struct Server {
    pub addr: SocketAddr,
    pub root: RootPath,
    map: RwLock<HashMap<RelPath, Weak<TraCell>>>,
    placeholder: Mutex<MaybeUninit<Arc<TraCell>>>,

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

        // put tc into place holder without initing
        let tc = TraCell::empty(&server, &RelPath::default(), None).await;
        server.placeholder.lock().await.write(tc.clone());
        server.add_tc(&tc).await;

        server
    }

    /// Activate the server.
    pub fn run(self: &Arc<Self>) -> Vec<JoinHandle<()>> {
        tokio::spawn(self.clone().init());

        let listen_handle = tokio::spawn(self.clone().listen());

        let server = Arc::downgrade(self);
        let src = self.root.clone();
        let tmp = Self::tmp_path();
        let mut watcher = Watcher::new(src, tmp, server);
        watcher.init();
        let watch_handle = tokio::task::spawn_blocking(move || watcher.watch());

        vec![listen_handle, watch_handle]
    }
}

// interface for local modification
impl Server {
    pub async fn init(self: Arc<Self>) {
        let tc = self.clone().make_tc(&RelPath::default()).await;
        tc.clone()
            .create(self.time.fetch_add(1, Ordering::AcqRel))
            .await;
    }

    pub async fn create(self: Arc<Self>, rel: RelPath) {
        let tc = self.clone().make_tc(&rel).await;
        tc.clone()
            .create(self.time.fetch_add(1, Ordering::AcqRel))
            .await;
        tc.clone().sendup_meta().await;
        tc.clone().broadcast().await;
    }

    pub async fn remove(self: Arc<Self>, rel: RelPath) {
        let tc = self.clone().make_tc(&rel).await;
        tc.clone()
            .remove(self.time.fetch_add(1, Ordering::AcqRel))
            .await;
        tc.clone().sendup_meta().await;
        tc.clone().broadcast().await;
    }

    pub async fn modify(self: Arc<Self>, rel: RelPath) {
        let tc = self.clone().make_tc(&rel).await;
        tc.clone()
            .modify(self.time.fetch_add(1, Ordering::AcqRel))
            .await;
        tc.clone().sendup_meta().await;
        tc.clone().broadcast().await;
    }
}

impl Server {
    /// Listen to the upcoming connections.
    #[instrument]
    pub async fn listen(self: Arc<Self>) {
        let listener = TcpListener::bind(self.addr).await.unwrap();

        info!("listener started");

        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let server = self.clone();

            tokio::spawn(async move {
                let mut buf = Vec::new();
                let n = stream.read_to_end(&mut buf).await.unwrap();

                if n != 0 {
                    let req = bincode::deserialize::<Request>(&buf).unwrap();
                    info!("{:?} recvs {:?}", server, req);
                    match req {
                        Request::ReadCell(path) => {
                            let cell = server.get_tc(&path).await.unwrap();
                            let res = Response::Cell(cell.into_rc(server.addr).await);
                            let res = bincode::serialize(&res).unwrap();
                            stream.write(&res).await.unwrap();
                        }
                        Request::ReadFile(path) => {
                            // TODO: I should not use unwrap here
                            let path = &server.root + &path;
                            let mut file = File::open(path.as_path_buf()).await.unwrap();

                            // TODO: use frame to optimize
                            let mut buf = Vec::new();
                            file.read_to_end(&mut buf).await.unwrap();
                            let res = Response::File(buf);
                            let res = bincode::serialize(&res).unwrap();
                            stream.write(&res).await.unwrap();
                        }
                        Request::SyncCell(peer, path) => {
                            let tc = server.make_tc(&path).await;
                            tc.sync(peer.addr, path).await;
                            let res = bincode::serialize(&Response::Sync).unwrap();
                            stream.write(&res).await.unwrap();
                        }
                    }
                    stream.shutdown().await.unwrap();
                } else {
                    println!("Find a empty connection");
                }
            });
        }
    }
}

impl Server {
    /// Get the `SyncCell` from the server. Return `None` when there is none.
    pub async fn get_tc(&self, path: &RelPath) -> Option<Arc<TraCell>> {
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
    pub async fn make_tc(self: Arc<Self>, path: &RelPath) -> Arc<TraCell> {
        let tc = self.get_tc(path).await;
        if let Some(tc) = tc {
            tc
        } else {
            let parent = self.clone().make_tc(&path.parent()).await;
            TraCell::empty(&self, path, Some(Arc::downgrade(&parent))).await
        }
    }

    /// Add the `SyncCell` to the server `HashMap`.
    pub async fn add_tc(&self, cell: &Arc<TraCell>) {
        self.map
            .write()
            .await
            .insert(cell.rel.clone(), Arc::downgrade(cell));
    }
}

impl Server {
    pub fn tmp_path() -> RootPath {
        let mut path = home::home_dir().unwrap();
        path.push(".rfsync");
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
