use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
};

use super::{
    hashed::{Hashed, Md5},
    roll::RollingChecksum,
};

pub struct HashTableEntry {
    pub md5: Md5,
    pub offset: usize,
}

impl HashTableEntry {
    pub fn new(md5: Md5, offset: usize) -> Self {
        Self { md5, offset }
    }
}

pub struct HashTable(HashMap<RollingChecksum, Vec<HashTableEntry>>);

impl HashTable {
    pub fn new<T>(list: &T) -> Self
    where
        for<'a> &'a T: IntoIterator<Item = &'a Hashed>,
    {
        let mut table = HashMap::new();
        for (offset, block) in list.into_iter().enumerate() {
            let entry = HashTableEntry::new(block.md5, offset);
            table
                .entry(block.rolling)
                .or_insert_with(Vec::new)
                .push(entry);
        }

        Self(table)
    }
}

impl Deref for HashTable {
    type Target = HashMap<RollingChecksum, Vec<HashTableEntry>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for HashTable {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
