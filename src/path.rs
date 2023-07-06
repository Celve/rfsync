use std::{
    ffi::OsString,
    ops::{Add, Sub},
    path::PathBuf,
};

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

    pub fn as_path_buf(&self) -> PathBuf {
        self.path.clone()
    }
}

// operation rule for path

impl Add<&RelPath> for &RootPath {
    type Output = AbsPath;

    fn add(self, rhs: &RelPath) -> Self::Output {
        let mut path = self.path.clone();
        path.push(&rhs.path);
        AbsPath::new(path)
    }
}

impl Add<&RelPath> for &RelPath {
    type Output = RelPath;

    fn add(self, rhs: &RelPath) -> Self::Output {
        let mut path = self.path.clone();
        path.push(&rhs.path);
        RelPath::new(path)
    }
}

impl Add<&RelPath> for &AbsPath {
    type Output = AbsPath;

    fn add(self, rhs: &RelPath) -> Self::Output {
        let mut path = self.path.clone();
        path.push(&rhs.path);
        AbsPath::new(path)
    }
}

impl Sub<&AbsPath> for &AbsPath {
    type Output = Option<RelPath>;

    fn sub(self, rhs: &AbsPath) -> Self::Output {
        let path = self.path.clone();
        path.strip_prefix(&rhs.path)
            .ok()
            .map(|path| RelPath::new(path.to_path_buf()))
    }
}

impl Sub<&RootPath> for &AbsPath {
    type Output = Option<RelPath>;

    fn sub(self, rhs: &RootPath) -> Self::Output {
        let path = self.path.clone();
        path.strip_prefix(&rhs.path)
            .ok()
            .map(|path| RelPath::new(path.to_path_buf()))
    }
}

impl Sub<&RelPath> for &AbsPath {
    type Output = Option<AbsPath>;

    fn sub(self, rhs: &RelPath) -> Self::Output {
        let mut path = self.path.clone();
        let mut suf = rhs.path.clone();
        if path.ends_with(&suf) {
            while suf.pop() {
                path.pop();
            }
            Some(AbsPath::new(path))
        } else {
            None
        }
    }
}
