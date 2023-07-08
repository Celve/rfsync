use serde::{Deserialize, Serialize};

use crate::{path::RelPath, peer::Peer, remote::RemoteCell};

#[derive(Deserialize, Serialize)]
pub enum Request {
    ReadCell(RelPath),
    ReadFile(RelPath),
    SyncCell(Peer, RelPath),
}

#[derive(Deserialize, Serialize)]
pub enum Response {
    Cell(RemoteCell),
    #[serde(with = "serde_bytes")]
    File(Vec<u8>),
    Sync,
    Err(String),
}
