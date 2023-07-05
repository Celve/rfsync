use std::{
    collections::HashMap,
    fmt::Debug,
    mem::MaybeUninit,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Weak,
    },
};

use async_recursion::async_recursion;
use tokio::{
    fs::{self, metadata},
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{Mutex, RwLock},
    task::JoinHandle,
};
use tracing::{event, info, instrument, Level};

use crate::{
    op::{Request, Response},
    sync::{CellType, SyncCell},
    time::VecTime,
    watcher::Watcher,
};

pub struct Server {
    pub addr: SocketAddr,
    pub path: PathBuf,
    cells: RwLock<HashMap<PathBuf, Weak<SyncCell>>>,
    root: Mutex<MaybeUninit<Arc<SyncCell>>>,

    /// The `time - 1` represents the last time the server is updated.
    pub time: AtomicUsize,
    pub id: usize,
}

impl Server {
    pub async fn new(addr: SocketAddr, path: PathBuf, id: usize) -> Arc<Self> {
        let server = Arc::new(Self {
            addr,
            path: path.clone(),
            cells: RwLock::new(HashMap::new()),
            root: Mutex::new(MaybeUninit::uninit()),
            time: AtomicUsize::new(0),
            id,
        });

        let cell = server.new_sc(&PathBuf::new(), None, CellType::Dir).await;
        server.root.lock().await.write(cell);

        server
    }

    /// Activate the server.
    pub fn run(self: &Arc<Self>) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::new();
        handles.push(tokio::spawn(self.clone().watch()));
        handles.push(tokio::spawn(self.clone().listen()));
        handles
    }
}

// interface
impl Server {
    /// Create a new file or directory. Increase the server time while creating.
    ///
    /// If it's a directory, it would recurse down, adding all of its component to the server.
    #[instrument]
    #[async_recursion]
    pub async fn create(self: Arc<Self>, path: &PathBuf, ty: CellType) -> Arc<SyncCell> {
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

        // if it's a directory, recurse it down
        if ty == CellType::Dir {
            let f = fs::read_dir(&cell.path).await;
            let mut cell_guard = cell.lock().await;
            if let Ok(mut stream) = f {
                while let Some(entry) = stream.next_entry().await.unwrap() {
                    let path = entry.path();
                    let ty = if path.is_dir() {
                        CellType::Dir
                    } else {
                        CellType::File
                    };

                    // recurse down and collect children
                    let child = Self::create(self.clone(), &path, ty).await;
                    cell_guard.children.push(child);
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
    pub async fn remove(self: Arc<Self>, path: &PathBuf) {
        event!(Level::INFO, "local remove: {:?}", path);
        let cell = self.get_sc(path).await;
        if let Some(cell) = cell {
            {
                let mut cell_guard = cell.lock().await;

                if cell_guard.ty != CellType::None {
                    // update metadata
                    cell_guard.ty = CellType::None;
                    cell_guard
                        .modif
                        .insert(self.id, self.time.fetch_add(1, Ordering::AcqRel));

                    // remote children
                    cell_guard.children.iter().for_each(|child| {
                        let path = child.path.clone();
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
    pub async fn modify(self: Arc<Self>, path: &PathBuf) {
        event!(Level::INFO, "local modify: {:?}", path);
        let cell = self.get_sc(path).await;
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
}

impl Server {
    /// Monitor the file system.
    #[instrument]
    pub async fn watch(self: Arc<Self>) {
        let mut watcher = Watcher::new("./".into());
        let mut rx = watcher.subscribe();

        watcher.watch();

        info!("watcher started");

        while let Some(event) = rx.recv().await {
            info!("watcher recvs event: {:?}", event);
            let path = event.path;
            match event.ty {
                crate::watcher::FileEventType::Create => {
                    let metadata = metadata(&path).await;
                    if let Ok(metadata) = metadata {
                        let ty = if metadata.is_dir() {
                            CellType::Dir
                        } else {
                            CellType::File
                        };

                        let cell = self.clone().create(&path, ty).await;
                        info!("create {:?}", cell);
                    }
                }
                crate::watcher::FileEventType::Delete => {
                    self.clone().remove(&path).await;
                    let cell = self.get_sc(&path).await.unwrap();
                    info!("remove {:?}", cell);
                }
                crate::watcher::FileEventType::Modify => {
                    self.clone().modify(&path).await;
                    event!(Level::INFO, "todo send sync to other server")
                }
            }
        }
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
                        Request::ReadFile(_) => todo!(),
                        Request::SyncDir(_) => todo!(),
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
        path: &PathBuf,
        parent: Option<Weak<SyncCell>>,
        ty: CellType,
    ) -> Arc<SyncCell> {
        let time = self.time.fetch_add(1, Ordering::AcqRel);
        let modif = VecTime::init(self.id, time);
        let cell = Arc::new(SyncCell::new(
            &self,
            path,
            parent.clone(),
            ty,
            modif,
            VecTime::new(),
            time,
            Vec::new(),
        ));

        // maintain the file tree
        if let Some(parent) = parent {
            parent
                .upgrade()
                .unwrap()
                .lock()
                .await
                .children
                .push(cell.clone());
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
        path: &PathBuf,
        ty: CellType,
    ) -> Arc<SyncCell> {
        // TODO: check parent instead of checking server
        let cell = self.get_sc(path).await;
        if let Some(cell) = cell {
            cell
        } else {
            self.new_sc(path, Some(Arc::downgrade(&parent)), ty).await
        }
    }

    /// Fetch the `SyncCell` from server according to the given path.
    /// If there is none, create a new one with server time increased.
    pub async fn make_sc_from_path(self: Arc<Self>, path: &PathBuf, ty: CellType) -> Arc<SyncCell> {
        // TODO: check parent instead of checking server
        let cell = self.get_sc(path).await;
        if let Some(cell) = cell {
            cell
        } else {
            let parent_path = path.parent().unwrap().to_path_buf();
            let parent = self
                .get_sc(&parent_path)
                .await
                .expect(format!("parent not found: {:?}", parent_path).as_str());
            self.new_sc(path, Some(Arc::downgrade(&parent)), ty).await
        }
    }
}

impl Server {
    /// Get the `SyncCell` from the server. Return `None` when there is none.
    pub async fn get_sc(&self, path: &PathBuf) -> Option<Arc<SyncCell>> {
        self.cells
            .read()
            .await
            .get(path)
            .map(|cell| cell.upgrade().unwrap().clone())
    }

    /// Add the `SyncCell` to the server `HashMap`.
    pub async fn add_sc(&self, cell: &Arc<SyncCell>) {
        self.cells
            .write()
            .await
            .insert(cell.path.clone(), Arc::downgrade(cell));
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
