use std::{ffi::OsString, path::PathBuf};

use serde::{Deserialize, Serialize};

/// The `RootPath` type is only used for the root path that the watcher is watching.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RootPath {
    path: PathBuf,
}

/// The `RelativePath` type is only used for the relative path that the watcher is watching.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelPath {
    path: PathBuf,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AbsPath {
    path: PathBuf,
}

impl RootPath {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn concat(&self, relative: &RelPath) -> AbsPath {
        let mut path = self.path.clone();
        path.push(&relative.path);
        AbsPath::new(path)
    }
}

impl From<PathBuf> for RootPath {
    fn from(value: PathBuf) -> Self {
        Self::new(value)
    }
}

impl From<RootPath> for PathBuf {
    fn from(value: RootPath) -> Self {
        value.path
    }
}

impl RelPath {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn concat(&self, other: &Self) -> RelPath {
        let mut path = self.path.clone();
        path.push(&other.path);
        RelPath::new(path)
    }

    pub fn parent(&self) -> Self {
        let mut path = self.path.clone();
        path.pop();
        RelPath::new(path)
    }
}

impl From<PathBuf> for RelPath {
    fn from(value: PathBuf) -> Self {
        Self::new(value)
    }
}

impl From<OsString> for RelPath {
    fn from(value: OsString) -> Self {
        Self::new(value.into())
    }
}

impl AbsPath {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn concat(&self, relative: &RelPath) -> Self {
        let mut path = self.path.clone();
        path.push(&relative.path);
        Self::new(path)
    }

    pub fn as_path_buf(&self) -> PathBuf {
        self.path.clone()
    }
}
