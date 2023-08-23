use std::path::PathBuf;

use super::{sync::SyncCell, time::VecTime};

pub struct LeanCell {
    pub modif: VecTime,
    pub sync: VecTime,
    pub path: PathBuf,
}

pub trait LeanCelled {
    fn modif(&self) -> &VecTime;
    fn sync(&self) -> &VecTime;
    fn path(&self) -> &PathBuf;
}

impl From<&SyncCell> for LeanCell {
    fn from(value: &SyncCell) -> Self {
        Self {
            modif: value.modif().clone(),
            sync: value.sync().clone(),
            path: value.path().clone(),
        }
    }
}

impl LeanCelled for LeanCell {
    fn modif(&self) -> &VecTime {
        &self.modif
    }

    fn sync(&self) -> &VecTime {
        &self.sync
    }

    fn path(&self) -> &PathBuf {
        &self.path
    }
}
