use serde::{Deserialize, Serialize};

use crate::{path::RelativePath, remote::RemoteCell};

#[derive(Deserialize, Serialize)]
pub enum Request {
    ReadCell(RelativePath),
    ReadFile(RelativePath),
    SyncDir(RelativePath),
}

#[derive(Deserialize, Serialize)]
pub enum Response {
    Cell(RemoteCell),
    #[serde(with = "serde_bytes")]
    File(Vec<u8>),
    Sync,
    Err(String),
}
