mod common;

use std::time::Duration;

use common::server_path;
use futures_util::future::join_all;
use tokio::{
    fs,
    time::{sleep_until, Instant},
};
use tracing::info;

use crate::common::{generate_server, init_src_dirs, init_tmp_dirs};

#[tokio::test]
async fn single_modif_before_start() {
    // init the subscriber
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    init_tmp_dirs().await;
    init_src_dirs(2).await;

    // make sure test env is cleared
    sleep_until(Instant::now() + Duration::from_millis(500)).await;

    // test
    info!("create dir test in server 0");
    fs::create_dir_all(server_path(0).join("test"))
        .await
        .unwrap();
    fs::write(server_path(0).join("test").join("test.txt"), "test")
        .await
        .unwrap();

    // make sure file is written
    sleep_until(Instant::now() + Duration::from_millis(500)).await;

    // init two server
    let (_, handles) = generate_server(2).await;

    // make sure that all servers are inited
    sleep_until(Instant::now() + Duration::from_millis(500)).await;

    join_all(handles).await;
}

#[tokio::test]
async fn single_modif_after_start() {
    // init the subscriber
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    init_tmp_dirs().await;
    init_src_dirs(2).await;

    // make sure test env is cleared
    sleep_until(Instant::now() + Duration::from_millis(500)).await;

    // init two server
    let (_, handles) = generate_server(2).await;

    // make sure server is started
    sleep_until(Instant::now() + Duration::from_millis(500)).await;

    // test
    info!("create dir test in server 0");
    fs::create_dir_all(server_path(0).join("test"))
        .await
        .unwrap();
    fs::write(server_path(0).join("test").join("test.txt"), "test")
        .await
        .unwrap();

    // make sure that all servers are inited
    sleep_until(Instant::now() + Duration::from_millis(500)).await;

    join_all(handles).await;
}

#[tokio::test]
async fn sync_diff_modif_simple() {
    // init the subscriber
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    init_tmp_dirs().await;
    init_src_dirs(2).await;

    // make sure test env is cleared
    sleep_until(Instant::now() + Duration::from_millis(500)).await;

    // init two server
    let (_, handles) = generate_server(2).await;

    // make sure server is started
    sleep_until(Instant::now() + Duration::from_millis(500)).await;

    // test
    info!("begin to create files");
    for i in 0..2 {
        // create in server 0
        tokio::spawn(fs::write(
            server_path(i % 2).join(format!("test{}.txt", i)),
            format!("test{}", i),
        ));
    }

    // make sure that all servers are inited
    sleep_until(Instant::now() + Duration::from_millis(500)).await;

    join_all(handles).await;
}

#[tokio::test]
async fn sync_diff_modif_hard() {
    // init the subscriber
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    init_tmp_dirs().await;
    init_src_dirs(2).await;

    // make sure test env is cleared
    sleep_until(Instant::now() + Duration::from_millis(500)).await;

    // init two server
    let (_, handles) = generate_server(2).await;

    // make sure server is started
    sleep_until(Instant::now() + Duration::from_millis(500)).await;

    // test
    info!("begin to create files");
    for i in 0..100 {
        // create in server 0
        tokio::spawn(fs::write(
            server_path(i % 2).join(format!("test{}.txt", i)),
            format!("test{}", i),
        ));
    }

    // make sure that all servers are inited
    sleep_until(Instant::now() + Duration::from_millis(500)).await;

    join_all(handles).await;
}
