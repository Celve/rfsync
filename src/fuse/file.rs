use std::{
    ops::{Deref, DerefMut},
    path::PathBuf,
};

use tokio::fs::{self, File};
use tracing::info;

use crate::subset::guard::{SubsetReadGuard, SubsetWriteGuard};

pub struct FileReadGuard<'a, const S: usize> {
    file: File,
    path: PathBuf,
    _guard: SubsetReadGuard<'a, u64, S>,
}

pub struct FileWriteGuard<'a, const S: usize> {
    file: File,
    path: PathBuf,
    _guard: SubsetWriteGuard<'a, u64, S>,
}

impl<'a, const S: usize> FileReadGuard<'a, S> {
    pub fn new(file: File, path: PathBuf, _guard: SubsetReadGuard<'a, u64, S>) -> Self {
        Self { file, path, _guard }
    }

    pub async fn upgrade(self) -> FileWriteGuard<'a, S> {
        FileWriteGuard::new(self.file, self.path, self._guard.upgrade().await)
    }
}

impl<'a, const S: usize> Deref for FileReadGuard<'a, S> {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl<'a, const S: usize> DerefMut for FileReadGuard<'a, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}

impl<'a, const S: usize> FileWriteGuard<'a, S> {
    pub fn new(file: File, path: PathBuf, _guard: SubsetWriteGuard<'a, u64, S>) -> Self {
        Self { file, path, _guard }
    }

    pub async fn destroy(self) {
        info!("remove file {:?}", self.path);
        fs::remove_file(&self.path)
            .await
            .expect("try to remove non-existing file");
    }
}

impl<'a, const S: usize> Deref for FileWriteGuard<'a, S> {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl<'a, const S: usize> DerefMut for FileWriteGuard<'a, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}
