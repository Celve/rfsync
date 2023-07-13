use std::{
    collections::HashMap,
    ffi::OsStr,
    fmt::Debug,
    fs,
    ops::{Add, Sub},
    path::PathBuf,
    sync::Weak,
};

use inotify::{Event, EventMask, Inotify, WatchDescriptor, WatchMask};
use tracing::{error, info, instrument};

use crate::{
    path::{AbsPath, RelPath, RootPath},
    server::Server,
};

const EVENT_BUFFER_SIZE: usize = 1024;

pub struct Watcher {
    src: RootPath,
    tmp: RootPath,
    srv: Weak<Server>,
    inotify: Inotify,
    wd2pair: HashMap<WatchDescriptor, (RelPath, bool)>,
}

impl Watcher {
    /// The `WatchMask` for src directory.
    const SRC_WM: WatchMask = WatchMask::CREATE
        .union(WatchMask::DELETE)
        .union(WatchMask::MODIFY)
        .union(WatchMask::MOVE);

    /// The `WatchMask` for tmp directory.
    const TMP_WM: WatchMask = WatchMask::MOVE.union(WatchMask::CREATE);

    pub fn new(src: RootPath, tmp: RootPath, srv: Weak<Server>) -> Self {
        let inotify = Inotify::init().expect("fail to init inotify");
        Self {
            src,
            tmp,
            srv,
            inotify,
            wd2pair: HashMap::new(),
        }
    }

    /// Initialize the watcher.
    ///
    /// This procedure is necessary before starting to watch.
    ///
    /// Spawn the function with `task::spawn_blocking`.
    /// Although it could be written with async, I just write it with blocking.
    /// For avoid tokio in the struct.
    #[instrument]
    pub fn init(&mut self) {
        // add src dir to map
        self.add_watch(&RelPath::default(), true);

        // init the `/tmp/rfsync` dir
        let res = fs::create_dir_all(self.tmp.as_path_buf());
        if let Err(e) = res {
            error!("{:?}", e);
        }

        // add tmp dir to map
        self.add_watch(&RelPath::default(), false);

        self.recurse_watch(&self.src.as_path_buf(), true);
        self.recurse_watch(&self.tmp.as_path_buf(), false);
        info!("watcher init");
    }

    /// Spawn the function with `tokio::task::spawn_blocking`.
    /// Because the inotify is OS-level utilities, which could not be optimized with async.
    #[instrument]
    pub fn watch(&mut self) {
        let mut buffer = [0; EVENT_BUFFER_SIZE];
        loop {
            let res = self.inotify.read_events_blocking(&mut buffer);
            match res {
                Ok(events) => {
                    let events: Vec<Event<&OsStr>> = events.collect();
                    self.delegate(&events);
                }
                Err(err) => {
                    error!("{:?}", err);
                }
            }
        }
    }

    /// Delegate stuffs to the corresponding function.
    #[instrument]
    fn delegate(&mut self, events: &Vec<Event<&OsStr>>) {
        let len = events.len();
        let mut i = 0;
        while i < len {
            let event = &events[i];
            if i < len - 1 && self.is_ignored(event, &events[i + 1]) {
                i += 2;
            } else {
                info!("{:?}", event);
                let (prel, is_src) = self.wd2pair.get(&event.wd).map(|x| x.clone()).unwrap();
                let rel = prel.add(&event.name.map_or(RelPath::default(), |x| RelPath::from(x)));

                if is_src {
                    self.solve_src_event(&event, &rel);
                } else {
                    self.solve_tmp_event(&event, &rel);
                }

                i += 1;
            }
        }
    }

    /// The function is delegated to do the stuff relating to src directory.
    fn solve_src_event(&mut self, event: &Event<&OsStr>, rel: &RelPath) {
        // for inotify
        if event.mask.contains(EventMask::CREATE | EventMask::ISDIR) {
            self.add_watch(rel, true);
        }

        let srv = self.srv.upgrade().unwrap();
        if event.mask.contains(EventMask::CREATE) || event.mask.contains(EventMask::MOVED_TO) {
            tokio::spawn(srv.create(rel.clone()));
        } else if event.mask.contains(EventMask::DELETE) {
            tokio::spawn(srv.remove(rel.clone()));
        } else if event.mask.contains(EventMask::MODIFY) {
            tokio::spawn(srv.modify(rel.clone()));
        } else if event.mask.contains(EventMask::IGNORED) {
            // the watch descriptor is no longer valid
            self.wd2pair.remove(&event.wd);
        }
    }

    /// The function is delegated to do the stuff relating to tmp directory.
    fn solve_tmp_event(&mut self, event: &Event<&OsStr>, rel: &RelPath) {
        // for inotify
        if event.mask.contains(EventMask::CREATE | EventMask::ISDIR) {
            self.add_watch(rel, false);
        }

        if event.mask.contains(EventMask::MOVED_FROM) {
            todo!()
        } else if event.mask.contains(EventMask::MOVED_TO) {
            todo!()
        } else {
            // the watch descriptor is no longer valid
            self.wd2pair.remove(&event.wd);
        }
    }

    /// Add watch on the corresponding relative path.
    ///
    /// Because the `WatchMask` for the src and tmp directory is different, `is_src` field is used to differentiate this.
    #[instrument]
    fn add_watch(&mut self, rel: &RelPath, is_src: bool) {
        let (abs, wm) = if is_src {
            (self.src.add(&rel), Self::SRC_WM)
        } else {
            (self.tmp.add(&rel), Self::TMP_WM)
        };
        info!("add {:?}", abs);
        let wd = self.inotify.watches().add(&abs.as_path_buf(), wm).unwrap();
        self.wd2pair.insert(wd, (rel.clone(), is_src));
    }

    /// Recursively add watch on the corresponding path. The function would only be called at initialization.
    #[instrument]
    fn recurse_watch(&mut self, path: &PathBuf, is_src: bool) {
        let dir = fs::read_dir(path).unwrap();
        for entry in dir {
            let entry = entry.unwrap();
            let path = entry.path();
            info!("{:?}", path);
            if path.is_dir() {
                let rel = AbsPath::new(path.clone())
                    .sub(if is_src { &self.src } else { &self.tmp })
                    .unwrap();
                self.add_watch(&rel, is_src);
                self.recurse_watch(&path, is_src);
            }
        }
    }

    /// Judge whether the two events are relavent.
    ///
    /// A relavent event is defined as the first one is the `MOVED_FROM` for the second one.
    /// And the second one if the `MOVED_TO` for the first one.
    ///
    /// It's believed that only the `MOVED_FROM` and `MOVED_TO` pair would share the same cookie.
    /// Other events except `MOVE` would not have cookie.
    ///
    /// It is promised in the Linux man that the exchange is atomic, so I think it would be in the same buffer.
    fn is_ignored(&self, lhs: &Event<&OsStr>, rhs: &Event<&OsStr>) -> bool {
        if lhs.cookie == rhs.cookie {
            let (lhs_rel, lhs_src) = self.wd2pair.get(&lhs.wd).unwrap();
            let (rhs_rel, rhs_src) = self.wd2pair.get(&rhs.wd).unwrap();
            lhs_src != rhs_src && lhs_rel == rhs_rel
        } else {
            false
        }
    }
}

impl Debug for Watcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Watcher")
            .field("src", &self.src)
            .field("tmp", &self.tmp)
            .finish()
    }
}
