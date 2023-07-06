use std::{
    collections::HashMap,
    fmt::Debug,
    mem::MaybeUninit,
    net::SocketAddr,
    ops::Sub,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Weak,
    },
};

use async_recursion::async_recursion;
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{Mutex, RwLock},
    task::JoinHandle,
};
use tracing::{info, instrument};

use crate::{
    op::{Request, Response},
    path::{AbsPath, RelPath, RootPath},
    peer::{Peer, PeerList},
    remote::RemoteCell,
    sync::{CellType, SyncCell},
    time::VecTime,
};

pub struct Server {
    pub addr: SocketAddr,
    pub root: RootPath,
    map: RwLock<HashMap<RelPath, Weak<SyncCell>>>,
    placeholder: Mutex<MaybeUninit<Arc<SyncCell>>>,

    /// Other servers.
    pub peers: RwLock<PeerList>,

    /// The `time - 1` represents the last time the server is updated.
    pub time: AtomicUsize,
    pub id: usize,
}

impl Server {
    pub async fn new(addr: SocketAddr, path: &PathBuf, id: usize) -> Arc<Self> {
        let server = Arc::new(Self {
            addr,
            root: path.clone().into(),
            map: RwLock::new(HashMap::new()),
            placeholder: Mutex::new(MaybeUninit::uninit()),
            peers: RwLock::new(PeerList::new()),
            time: AtomicUsize::new(0),
            id,
        });

        let cell = server
            .new_sc(&RelPath::default(), None, CellType::Dir)
            .await;
        cell.clone().watch();

        info!("create {:?}", cell);

        // put the cell inside the placeholder
        server.placeholder.lock().await.write(cell);

        server
    }

    /// Activate the server.
    pub fn run(self: &Arc<Self>) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::new();
        handles.push(tokio::spawn(self.clone().listen()));
        handles
    }
}

// interface
impl Server {
    pub async fn init(self: Arc<Self>) {
        let cell = self.get_sc(&RelPath::default()).await.unwrap();
        let abs = cell.path();
        let dir = fs::read_dir(abs.as_path_buf()).await;
        if let Ok(mut stream) = dir {
            while let Some(entry) = stream.next_entry().await.unwrap() {
                let path = entry.path();
                let ty = if path.is_dir() {
                    CellType::Dir
                } else {
                    CellType::File
                };
                let path = AbsPath::new(path).sub(&self.root).unwrap();

                // recurse down and collect children
                let child = Self::create(self.clone(), &path, ty).await;
                cell.lock()
                    .await
                    .children
                    .insert(child.rel.clone(), child.clone());
            }
        }
    }

    /// Create a new file or directory. Increase the server time while creating.
    ///
    /// If it's a directory, it would recurse down, adding all of its component to the server.
    #[instrument]
    #[async_recursion]
    pub async fn create(self: Arc<Self>, path: &RelPath, ty: CellType) -> Arc<SyncCell> {
        let cell = self.get_sc(path).await;
        let cell = if let Some(cell) = cell {
            // it should be deleted before
            {
                let mut cell_guard = cell.lock().await;
                assert!(cell_guard.ty == CellType::None);
                cell_guard.ty = ty;

                // TODO: I don't know whether the algorithm is correct
                let time = self.time.fetch_add(1, Ordering::AcqRel);
                cell_guard.crt = time;
                cell_guard.modif.insert(self.id, time);
            }
            cell
        } else {
            // the file has never been created before
            self.clone().make_sc_from_path(path, ty).await
        };

        info!("create {:?}", cell);

        cell.clone().watch();

        // if it's a directory, recurse it down
        if ty == CellType::Dir {
            let f = fs::read_dir(cell.path().as_path_buf()).await;
            let mut cell_guard = cell.lock().await;
            if let Ok(mut stream) = f {
                while let Some(entry) = stream.next_entry().await.unwrap() {
                    let path = entry.path();
                    let ty = if path.is_dir() {
                        CellType::Dir
                    } else {
                        CellType::File
                    };
                    let path = RelPath::new(path);

                    // recurse down and collect children
                    let child = Self::create(self.clone(), &path, ty).await;
                    cell_guard.children.insert(child.rel.clone(), child.clone());
                }
            }
        }

        // send up metadata
        cell.clone().sendup_meta().await;

        cell
    }

    /// Remove a file or directory because of the notice of the file system.
    #[instrument]
    #[async_recursion]
    pub async fn remove(self: Arc<Self>, path: &RelPath) {
        let cell = self.get_sc(path).await;
        if let Some(cell) = cell {
            {
                let mut cell_guard = cell.lock().await;

                if cell_guard.ty != CellType::None {
                    info!("remove {:?}", cell);

                    // update metadata
                    cell_guard.ty = CellType::None;
                    cell_guard
                        .modif
                        .insert(self.id, self.time.fetch_add(1, Ordering::AcqRel));

                    // remote children
                    cell_guard.children.iter().for_each(|(path, _)| {
                        let path = path.clone();
                        let server = self.clone();
                        tokio::spawn(async move { server.remove(&path).await });
                    });

                    // for now, we don't remove the children from the parent
                    // cell_guard.children.clear();
                }
            }

            cell.clone().sendup_meta().await;
        } else {
            todo!("Remove should remove existing file")
        }
    }

    /// Deal with modification of file.
    #[instrument]
    pub async fn modify(self: Arc<Self>, path: &RelPath) {
        let cell = self.get_sc(path).await;
        info!("modify {:?}", path);
        if let Some(cell) = cell {
            let mut cell_guard = cell.lock().await;
            cell_guard
                .modif
                .insert(self.id, self.time.fetch_add(1, Ordering::AcqRel));

            cell.clone().sendup_meta().await;
        } else {
            todo!("Modify should modify existing file")
        }
    }

    /// Deal with synchronization of dir.
    pub async fn sync(self: Arc<Self>, peer: Peer, path: &RelPath) {
        let cell = self.make_sc_from_path(path, CellType::Dir).await;
        let rcell = RemoteCell::from_path(peer.addr, path.clone()).await;
        cell.sync_cell(rcell).await;
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
                // let mut buf = [0; RECV_BUFFER_SIZE];
                let mut buf = Vec::new();
                let n = stream.read_to_end(&mut buf).await.unwrap();

                if n != 0 {
                    let req = bincode::deserialize::<Request>(&buf).unwrap();
                    match req {
                        Request::ReadCell(path) => {
                            let cell = server.get_sc(&path).await.unwrap();
                            let res = Response::Cell(cell.into_rc(server.addr).await);
                            let res = bincode::serialize(&res).unwrap();
                            stream.write(&res).await.unwrap();
                        }
                        Request::ReadFile(path) => {
                            // TODO: I should not use unwrap here
                            // let path = server.root.concat(&path);
                            let path = &server.root + &path;
                            let mut file = File::open(path.as_path_buf()).await.unwrap();

                            // TODO: use frame to optimize
                            let mut buf = Vec::new();
                            file.read_to_end(&mut buf).await.unwrap();
                            let res = Response::File(buf);
                            let res = bincode::serialize(&res).unwrap();
                            stream.write(&res).await.unwrap();
                        }
                        Request::SyncDir(peer, path) => {
                            server.sync(peer, &path).await;
                        }
                    }
                } else {
                    println!("Find a empty connection");
                }
            });
        }
    }
}

impl Server {
    /// Create a new `SyncCell` for the given path.
    /// It should be made sure that the `SyncCell` is not existing in the server.
    ///
    /// The function is responsible for maintaining the file tree and its modification time.
    ///
    /// It should not be called dor the synchronization job. It's especially designed for  
    pub async fn new_sc(
        self: &Arc<Server>,
        path: &RelPath,
        parent: Option<Weak<SyncCell>>,
        ty: CellType,
    ) -> Arc<SyncCell> {
        let time = self.time.fetch_add(1, Ordering::AcqRel);
        let modif = VecTime::init(self.id, time);
        let cell = SyncCell::new(
            &self,
            path,
            parent.clone(),
            ty,
            modif,
            VecTime::new(),
            time,
            HashMap::new(),
        );

        // maintain the file tree
        if let Some(parent) = parent {
            parent
                .upgrade()
                .unwrap()
                .lock()
                .await
                .children
                .insert(cell.rel.clone(), cell.clone());
        }

        // add to the server
        self.add_sc(&cell).await;

        cell
    }

    /// Fetch the `SyncCell` from the parent.
    /// If there is none, create a new one with server time increased.
    ///
    /// If there is no such meta data, create a new one according to the give type.
    pub async fn make_sc_from_parent(
        self: Arc<Self>,
        parent: Arc<SyncCell>,
        relative: &RelPath,
        ty: CellType,
    ) -> Arc<SyncCell> {
        // TODO: check parent instead of checking server
        let cell = self.get_sc(relative).await;
        if let Some(cell) = cell {
            cell
        } else {
            self.new_sc(relative, Some(Arc::downgrade(&parent)), ty)
                .await
        }
    }

    /// Fetch the `SyncCell` from server according to the given path.
    /// If there is none, create a new one with server time increased.
    pub async fn make_sc_from_path(self: Arc<Self>, rel: &RelPath, ty: CellType) -> Arc<SyncCell> {
        // TODO: check parent instead of checking server
        let cell = self.get_sc(rel).await;
        if let Some(cell) = cell {
            cell
        } else {
            let parent_rel = rel.parent();
            let parent = self
                .get_sc(&parent_rel)
                .await
                .expect(format!("parent not found: {:?}", parent_rel).as_str());
            self.new_sc(rel, Some(Arc::downgrade(&parent)), ty).await
        }
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

    /// Add the `SyncCell` to the server `HashMap`.
    pub async fn add_sc(&self, cell: &Arc<SyncCell>) {
        self.map
            .write()
            .await
            .insert(cell.rel.clone(), Arc::downgrade(cell));
    }
}

impl Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("id", &self.id)
            // .field("addr", &self.addr)
            // .field("path", &self.path)
            .field("time", &self.time)
            .finish()
    }
}
