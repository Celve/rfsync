use super::subscribe::Observer;
use futures_util::StreamExt;
use inotify::{EventMask, Inotify, WatchMask};
use std::path::PathBuf;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::{Receiver, Sender};

const MPSC_BUFFER_SIZE: usize = 16;
const STREAM_BUFFER_SIZE: usize = 1024;

/// Watcher keeps its eyes on the specified directory, noticing all subscribers when changes occur in its directory subtree.
///
/// Watcher is fully depending on the crate `inotify`.
pub struct Watcher {
    /// The path to be watched, which could not be modified after initiation.
    #[allow(unused)]
    path: PathBuf,

    /// Receiver for inotify.
    rx: Receiver<FileEvent>,

    /// Senders for subscribers.
    txs: Vec<Sender<FileEvent>>,
}

/// File event is used to describe changes happened in the file system.
/// It's a wrapper of the event from inotify.
#[derive(Clone, Debug)]
pub struct FileEvent {
    path: PathBuf,
    event: EventMask,
}

impl Watcher {
    pub fn new(path: PathBuf) -> Self {
        let (tx, rx) = channel::<FileEvent>(MPSC_BUFFER_SIZE);
        let path_replica = path.clone();

        // the notification of inotify is handled by tokio
        tokio::spawn(async move {
            // init the inotify instance with the help of Linux
            let inotify = Inotify::init().expect("Failed to initialize inotify.");
            inotify
                .watches()
                .add(
                    path_replica,
                    WatchMask::CREATE | WatchMask::DELETE | WatchMask::MODIFY | WatchMask::MOVE,
                )
                .unwrap();

            // establish the stream for inotify, using mpsc to relay the event
            // cannot use `Vec` as buffer
            let mut buffer = [0; STREAM_BUFFER_SIZE];
            let mut stream = inotify.into_event_stream(&mut buffer).unwrap();
            while let Some(e) = stream.next().await {
                match e {
                    Ok(event) => {
                        // we maintain the event and the path inside the file event
                        tx.send(FileEvent::new(
                            event.name.map_or(PathBuf::new(), |x| x.into()),
                            event.mask,
                        ))
                        .await
                        .expect("Failed to send event to the channel.");
                    }
                    Err(_) => panic!("Failed to get the vent from inotify."),
                }
            }
        });

        Self {
            path,
            rx,
            txs: Vec::new(),
        }
    }

    /// Give way to watching.
    /// Once it's initiated, the watcher is scheduled by the tokio to watch the directory.
    pub fn watch(mut self) {
        tokio::spawn(async move {
            while let Some(event) = self.rx.recv().await {
                for tx in self.txs.iter_mut() {
                    tx.send(event.clone())
                        .await
                        .expect("Failed to send event to the channel.");
                }
            }
        });
    }
}

impl Observer for Watcher {
    type Event = FileEvent;

    fn observe(&mut self) -> Receiver<Self::Event> {
        let (tx, rx) = channel::<FileEvent>(MPSC_BUFFER_SIZE);
        self.txs.push(tx);
        rx
    }
}

impl FileEvent {
    pub fn new(path: PathBuf, event: EventMask) -> Self {
        Self { path, event }
    }
}
