use std::{
    io::SeekFrom,
    ops::{Deref, DerefMut},
};

use md5::Digest;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::fuse::meta::PAGE_SIZE;

use super::roll::{self, RollingChecksum};

#[derive(Deserialize, Serialize, PartialEq, Eq, Clone, Copy, Debug)]
pub struct Md5([u8; 16]);

impl From<[u8; 16]> for Md5 {
    fn from(value: [u8; 16]) -> Self {
        Self(value)
    }
}

impl From<Md5> for [u8; 16] {
    fn from(value: Md5) -> Self {
        value.0
    }
}

impl From<Digest> for Md5 {
    fn from(value: Digest) -> Self {
        Self(value.into())
    }
}

impl Default for Md5 {
    fn default() -> Self {
        Self(md5::compute(&[0; PAGE_SIZE]).into())
    }
}

#[derive(PartialEq, Eq, Deserialize, Serialize, Clone, Default, Debug)]
pub struct Hashed {
    pub rolling: RollingChecksum,
    pub md5: Md5,
}

impl Hashed {
    pub fn new<T: AsRef<[u8]>>(page: &T) -> Self {
        let rolling = roll::compute(page);
        let md5 = md5::compute(page).into();
        Self { rolling, md5 }
    }
}

pub enum HashedDelta {
    Modify(usize, Vec<Hashed>),
    Shrink(usize),
}

#[derive(Deserialize, Serialize, Default, Clone, Debug)]
pub struct HashedList(Vec<Hashed>);

impl HashedList {
    pub async fn new<R, T>(reader: &mut R)
    where
        R: AsMut<T>,
        T: AsyncSeekExt + AsyncReadExt + Unpin,
    {
        let mut bytes = vec![0; PAGE_SIZE];
        let mut list = Vec::new();
        loop {
            let len = reader.as_mut().read(bytes.as_mut()).await.unwrap();
            if len < PAGE_SIZE {
                break;
            }

            list.push(Hashed::new(&bytes));

            // move cursor
            reader
                .as_mut()
                .seek(SeekFrom::Current(PAGE_SIZE as i64))
                .await
                .unwrap();
        }
    }
}

impl Deref for HashedList {
    type Target = Vec<Hashed>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for HashedList {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'a> IntoIterator for &'a HashedList {
    type Item = &'a Hashed;

    type IntoIter = std::slice::Iter<'a, Hashed>;

    fn into_iter(self) -> Self::IntoIter {
        (&self.0).into_iter()
    }
}
