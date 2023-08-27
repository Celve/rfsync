use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};

use crate::{
    comm::iter::Iterator,
    fuse::meta::PAGE_SIZE,
    rsync::{inst::Inst, roll::RollingCalculator},
};

use super::{
    hashed::{Hashed, Md5},
    inst::InstList,
    roll::RollingChecksum,
};

pub struct HashTableEntry {
    md5: Md5,
    offset: usize,
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

    pub fn reconstruct<'a, R, F>(&'a self, reader: &'a mut R) -> Reconstructor<F>
    where
        R: AsMut<F>,
        F: AsyncSeekExt + AsyncRead + Unpin,
    {
        Reconstructor::new(&self, reader.as_mut())
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

pub struct Reconstructor<'a, F>
where
    F: AsyncSeekExt + AsyncRead + Unpin,
{
    table: &'a HashTable,
    reader: &'a mut F,
    calculator: RollingCalculator,
    stack: Vec<u8>,
    bytes: Vec<u8>,
    pagenum: usize,
    end: bool,
}

impl<'a, F> Reconstructor<'a, F>
where
    F: AsyncSeekExt + AsyncRead + Unpin,
{
    pub fn new(table: &'a HashTable, reader: &'a mut F) -> Self {
        Self {
            table,
            reader,
            calculator: RollingCalculator::new(PAGE_SIZE as u16),
            stack: vec![],
            bytes: vec![0; PAGE_SIZE],
            pagenum: 0,
            end: false,
        }
    }
}

impl<'a, F> Iterator for Reconstructor<'a, F>
where
    F: AsyncSeekExt + AsyncRead + Unpin,
{
    type Item = InstList;

    async fn next(&mut self) -> Option<Self::Item> {
        if self.end {
            return None;
        }

        let len = self.reader.read(&mut self.bytes).await.unwrap();
        if len == 0 {
            self.end = true;
            return if self.stack.is_empty() {
                None
            } else {
                let mut insts = InstList::new();
                insts.push(Inst::Fill(self.stack.clone()));
                Some(insts)
            };
        }

        let mut insts = InstList::new();
        for i in 0..len {
            let byte = self.bytes[i];
            self.stack.push(byte);
            self.calculator.forward(byte);

            if let Some(rolling) = self.calculator.compute() {
                if let Some(entries) = self.table.get(&rolling) {
                    let md5: Md5 =
                        md5::compute(&self.stack[(self.stack.len() - PAGE_SIZE)..]).into();
                    let mut offset = None;
                    for entry in entries {
                        if entry.md5 == md5 {
                            if entry.offset >= self.pagenum {
                                offset = Some(entry.offset);
                                break;
                            } else if offset.is_none() {
                                offset = Some(entry.offset);
                            }
                        }
                    }

                    if let Some(offset) = offset {
                        if self.stack.len() > PAGE_SIZE {
                            self.stack.resize(self.stack.len() - PAGE_SIZE, 0);
                            insts.push(Inst::Fill(std::mem::take(&mut self.stack)));
                        }
                        insts.push(Inst::Copy(offset));
                        self.stack = Vec::new();
                        self.calculator.clear();
                    }
                }
            }
        }

        self.pagenum += 1;
        Some(insts)
    }
}
