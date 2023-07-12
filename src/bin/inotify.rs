use std::{path::PathBuf, sync::Weak};

use clap::Parser;
use rfsync::{path::RootPath, watcher::Watcher};
use tokio::task;
use tracing::instrument;

#[derive(Parser)]
struct Cli {
    /// The path to watch.
    id: usize,
}

#[instrument]
#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::fmt().compact().finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let src = RootPath::new(PathBuf::from("./debug"));
    let home = home::home_dir().unwrap();
    let tmp = RootPath::new(home.join(".rfsync"));

    let mut watcher = Watcher::new(src, tmp, Weak::new());
    watcher.init();
    task::spawn_blocking(move || {
        watcher.watch();
    });
}
