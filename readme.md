# rfsync
A remarkable file system synchronizer written in Rust, or a robust file system synchronizer with `rfsync` algorithm implemented. 

## Features
- Vector time pairs(modification vector time & synchronization vector time) used to maintain the status of files and directories, enabling partial synchronization without any futile efforts.
- A FUSE-based file system to support **logically strict** vector time pairs modifications and synchronizations with no chance of error, compared to normal `inotify`-based file system synchronizer.
- Auto modification detections and synchronizations. Every writes to the file system would be detected and synchronized to the remote file system immediately without any perceivable interrupt or locking on the current file system due to the design of the synchronization engine based on FUSE. There is not any perceptible difference with a normal file system, or a directory. 
- Use `rsync` algorithm to support incremental synchronization.
- Written in async Rust, with the help of `tokio`, `tonic`, and `fuser` crates.

## Usages
Two binaries is provided by the project: `rfsyncd` and `rfsync-cli`. 

`rfsyncd` is the daemon process that maintains the FUSE file system and the synchronization engine. Every `rfsyncd` has to be assigned with a unique ID, a socket address for synchronization, and a path to indicates which directory to be synchronized.

`rfsync-cli` can connect to the `rfsyncd` that run in anywhere, even in remote servers. By using the `join` command, it could make the two servers join to the same synchronization group and begins synchronize with each other. 

