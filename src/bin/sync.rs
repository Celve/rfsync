use rfsync::{subscribe::Observer, watcher::Watcher};

#[tokio::main]
async fn main() {
    let mut watcher = Watcher::new("./".into());
    let mut rx = watcher.observe();

    watcher.watch();

    while let Some(event) = rx.recv().await {
        println!("{:?}", event);
    }
}
