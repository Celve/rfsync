use std::{
    io::SeekFrom,
    ops::{Deref, DerefMut},
};

use md5::Digest;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::{fuse::meta::PAGE_SIZE, rpc::FakeHashed};

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

impl From<[u64; 2]> for Md5 {
    fn from(value: [u64; 2]) -> Self {
        let mut bytes = [0u8; 16];
        let (left, right) = bytes.split_at_mut(8);
        left.copy_from_slice(&value[0].to_le_bytes());
        right.copy_from_slice(&value[1].to_le_bytes());
        Self(bytes)
    }
}

impl From<Md5> for [u64; 2] {
    fn from(mut value: Md5) -> Self {
        let (left, right) = value.0.split_at(8);
        let (mut left_bytes, mut right_bytes) = ([0u8; 8], [0u8; 8]);
        left_bytes.copy_from_slice(left);
        right_bytes.copy_from_slice(right);

        [
            u64::from_le_bytes(left_bytes),
            u64::from_le_bytes(right_bytes),
        ]
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

impl From<&Hashed> for FakeHashed {
    fn from(value: &Hashed) -> Self {
        let md5 = <[u64; 2]>::from(value.md5);
        Self {
            small: value.rolling.into(),
            big1: md5[0],
            big2: md5[1],
        }
    }
}

impl From<&FakeHashed> for Hashed {
    fn from(value: &FakeHashed) -> Self {
        let md5 = [value.big1, value.big2];
        Self {
            rolling: value.small.into(),
            md5: md5.into(),
        }
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

impl From<Vec<Hashed>> for HashedList {
    fn from(value: Vec<Hashed>) -> Self {
        Self(value)
    }
}

impl From<&Vec<FakeHashed>> for HashedList {
    fn from(value: &Vec<FakeHashed>) -> Self {
        Self(value.iter().map(|h| h.into()).collect())
    }
}

impl From<HashedList> for Vec<Hashed> {
    fn from(value: HashedList) -> Self {
        value.0
    }
}

impl From<&HashedList> for Vec<FakeHashed> {
    fn from(value: &HashedList) -> Self {
        value.0.iter().map(|h| h.into()).collect()
    }
}
