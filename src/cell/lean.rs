use std::path::PathBuf;

use super::{sync::SyncCell, time::VecTime};

pub struct LeanCell {
    pub modif: VecTime,
    pub path: PathBuf,
}

impl From<&SyncCell> for LeanCell {
    fn from(value: &SyncCell) -> Self {
        Self {
            modif: value.modif.clone(),
            path: value.path.clone(),
        }
    }
}
