use clap::Parser;
use futures_util::StreamExt;
use inotify::{EventMask, Inotify, WatchMask};
use tracing::instrument;

const STREAM_BUFFER_SIZE: usize = 1024;

#[derive(Parser)]
struct Cli {
    /// The path to watch.
    id: usize,
}

#[instrument]
#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    println!("begin");
    let inotify = Inotify::init().expect("Failed to initialize inotify.");
    inotify
        .watches()
        .add(
            format_args!("./debug/{}", cli.id).to_string(),
            WatchMask::ALL_EVENTS,
        )
        .unwrap();

    let mut buf = [0; STREAM_BUFFER_SIZE];
    let mut stream = inotify.into_event_stream(&mut buf).unwrap();
    while let Some(e) = stream.next().await {
        match e {
            Ok(event) => {
                println!("receive {:?}", event);

                if event.mask.contains(EventMask::IGNORED) {
                    // the watcher should be reinited
                    break;
                }
            }
            Err(_) => panic!("Failed to get the event from inotify."),
        }
    }
}
