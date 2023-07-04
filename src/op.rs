use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::remote::RemoteCell;

#[derive(Deserialize, Serialize)]
pub enum Request {
    ReadCell(PathBuf),
    ReadFile(PathBuf),
    SyncDir(PathBuf),
}

#[derive(Deserialize, Serialize)]
pub enum Response {
    Cell(RemoteCell),
    #[serde(with = "serde_bytes")]
    File(Vec<u8>),
    Sync,
    Err(String),
}
