use std::{fs, io::ErrorKind, path::PathBuf};

use clap::Parser;
use fuser::MountOption;
use home::home_dir;
use log::{error, LevelFilter};
use rfsync::fuse::sync::{SyncFs, SyncFsConfig};

#[derive(Parser)]
struct Cli {
    /// The path of directory to be synchronized
    mount_point: PathBuf,
}

fn main() {
    let cli = Cli::parse();
    let mount_point = cli.mount_point;
    let options = vec![
        MountOption::FSName("syncfs".to_string()),
        MountOption::AutoUnmount,
        MountOption::AllowRoot,
    ];
    env_logger::builder()
        .format_timestamp_nanos()
        .filter_level(LevelFilter::Info)
        .init();
    let home = home_dir().unwrap().join(".syncfs");
    fs::create_dir_all(&home).unwrap();
    let fs = SyncFs::new(SyncFsConfig::new(home, true));
    let res = fuser::mount2(fs, mount_point.clone(), &options);

    if let Err(e) = res {
        // Return a special error code for permission denied, which usually indicates that
        // "user_allow_other" is missing from /etc/fuse.conf
        if e.kind() == ErrorKind::PermissionDenied {
            error!("{}", e.to_string());
            std::process::exit(2);
        }
    }
}
