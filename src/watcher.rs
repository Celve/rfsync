use futures_util::StreamExt;
use inotify::{EventMask, Inotify, WatchMask};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tracing::{info, instrument};

use crate::path::{RelativePath, RootPath};

const MPSC_BUFFER_SIZE: usize = 16;
const STREAM_BUFFER_SIZE: usize = 1024;

/// Watcher keeps its eyes on the specified directory, noticing all subscribers when changes occur in its directory subtree.
///
/// Watcher is fully depending on the crate `inotify`.
pub struct Watcher {
    /// The path to be watched, which could not be modified after initiation.
    root: RootPath,

    /// The path to be watched, which could not be modified after intit
    relative: RelativePath,

    /// Sender for inotify.
    tx: Sender<FileEvent>,

    /// Senders for subscribers.
    txs: Mutex<Vec<Sender<FileEvent>>>,
}

/// File event is used to describe changes happened in the file system.
/// It's a wrapper of the event from inotify.
#[derive(Clone, Debug)]
pub struct FileEvent {
    pub path: RelativePath,
    pub ty: FileEventType,
}

#[derive(Clone, Copy, Debug)]
pub enum FileEventType {
    Create,
    Delete,
    Modify,
}

impl Watcher {
    pub fn new(root: RootPath, relative: RelativePath) -> Arc<Self> {
        let (tx, rx) = channel::<FileEvent>(MPSC_BUFFER_SIZE);

        let watcher = Arc::new(Self {
            root,
            relative,
            tx: tx.clone(),
            txs: Mutex::new(Vec::new()),
        });

        // init the receiver
        tokio::spawn(watcher.clone().run_recv(rx));

        watcher
    }

    pub fn watch(self: Arc<Self>) {
        tokio::spawn(self.clone().run_inotify());
    }

    #[instrument]
    async fn run_inotify(self: Arc<Self>) {
        // init the inotify instance with the help of Linux
        let inotify = Inotify::init().expect("Failed to initialize inotify.");
        let path = self.root.concat(&self.relative);
        inotify
            .watches()
            .add(
                path.clone(),
                WatchMask::CREATE | WatchMask::DELETE | WatchMask::MODIFY | WatchMask::MOVE,
            )
            .unwrap();

        info!("watcher init");

        // establish the stream for inotify, using mpsc to relay the event
        // use `Vec` as buffer with specified capacity
        let mut buf = [0; STREAM_BUFFER_SIZE];
        let mut stream = inotify.into_event_stream(&mut buf).unwrap();
        while let Some(e) = stream.next().await {
            match e {
                Ok(event) => {
                    info!("watcher {:?} receives event {:?}", path, event);

                    if event.mask.contains(EventMask::IGNORED) {
                        // the watcher should be reinited
                        break;
                    }

                    let path = self
                        .relative
                        .concat(&event.name.map_or(RelativePath::default(), |x| x.into()));

                    // we maintain the event and the path inside the file event
                    self.tx
                        .send(FileEvent::new(path, event.mask.into()))
                        .await
                        .expect("Failed to send event to the channel.");
                }
                Err(_) => panic!("Failed to get the event from inotify."),
            }
        }

        info!("watcher exit");
    }

    /// Give way to watching.
    /// Once it's initiated, the watcher is scheduled by the tokio to watch the directory.
    pub async fn run_recv(self: Arc<Self>, mut rx: Receiver<FileEvent>) {
        while let Some(event) = rx.recv().await {
            for tx in self.txs.lock().await.iter_mut() {
                tx.send(event.clone())
                    .await
                    .expect("Failed to send event to the channel.");
            }
        }
    }

    pub async fn subscribe(&self) -> Receiver<FileEvent> {
        let (tx, rx) = channel::<FileEvent>(MPSC_BUFFER_SIZE);
        self.txs.lock().await.push(tx);
        rx
    }
}

impl Debug for Watcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Watcher")
            .field("root", &self.root)
            .field("relative", &self.relative)
            .finish()
    }
}

impl FileEvent {
    pub fn new(path: RelativePath, ty: FileEventType) -> Self {
        Self { path, ty }
    }
}

impl From<EventMask> for FileEventType {
    fn from(value: EventMask) -> Self {
        // it might contains something like `IS_DIR`, therefore we could not use the exact match
        if value.contains(EventMask::CREATE) || value.contains(EventMask::MOVED_TO) {
            Self::Create
        } else if value.contains(EventMask::DELETE) || value.contains(EventMask::MOVED_FROM) {
            Self::Delete
        } else if value.contains(EventMask::MODIFY) {
            Self::Modify
        } else {
            panic!("Unknown event type {:?}.", value)
        }
    }
}
