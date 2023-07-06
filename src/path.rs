use std::{ffi::OsString, path::PathBuf};

use serde::{Deserialize, Serialize};

/// The `RootPath` type is only used for the root path that the watcher is watching.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RootPath {
    path: PathBuf,
}

/// The `RelativePath` type is only used for the relative path that the watcher is watching.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelativePath {
    path: PathBuf,
}

impl RootPath {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn concat(&self, relative: &RelativePath) -> PathBuf {
        let mut path = self.path.clone();
        path.push(&relative.path);
        path
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

impl RelativePath {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn concat(&self, other: &Self) -> RelativePath {
        let mut path = self.path.clone();
        path.push(&other.path);
        RelativePath::new(path)
    }

    pub fn as_path_buf(&self) -> &PathBuf {
        &self.path
    }

    pub fn parent(&self) -> Self {
        let mut path = self.path.clone();
        path.pop();
        RelativePath::new(path)
    }
}

impl From<PathBuf> for RelativePath {
    fn from(value: PathBuf) -> Self {
        Self::new(value)
    }
}

impl From<OsString> for RelativePath {
    fn from(value: OsString) -> Self {
        Self::new(value.into())
    }
}
