use std::collections::HashMap;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};

use crate::{
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

pub struct HashTable {
    table: HashMap<RollingChecksum, Vec<HashTableEntry>>,
}

impl HashTableEntry {
    pub fn new(md5: Md5, offset: usize) -> Self {
        Self { md5, offset }
    }
}

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

        Self { table }
    }

    pub async fn reconstruct<R, T>(&self, reader: &mut R) -> InstList
    where
        R: AsMut<T>,
        T: AsyncSeekExt + AsyncRead + Unpin,
    {
        let mut calculator = RollingCalculator::new(PAGE_SIZE as u16);
        let mut stack = Vec::new();
        let mut bytes = vec![0; PAGE_SIZE];
        let mut insts = InstList::new();
        let mut pagenum = 0;

        loop {
            let len = reader.as_mut().read(&mut bytes).await.unwrap();

            for i in 0..len {
                let byte = bytes[i];
                stack.push(byte);
                calculator.forward(byte);

                if let Some(rolling) = calculator.compute() {
                    if let Some(entries) = self.table.get(&rolling) {
                        let md5: Md5 = md5::compute(&stack[(stack.len() - PAGE_SIZE)..]).into();
                        let mut offset = None;
                        for entry in entries {
                            if entry.md5 == md5 {
                                if entry.offset >= pagenum {
                                    offset = Some(entry.offset);
                                    break;
                                } else if offset.is_none() {
                                    offset = Some(entry.offset);
                                }
                            }
                        }

                        if let Some(offset) = offset {
                            if stack.len() > PAGE_SIZE {
                                stack.resize(stack.len() - PAGE_SIZE, 0);
                                insts.push(Inst::Fill(stack));
                            }
                            insts.push(Inst::Copy(offset));
                            stack = Vec::new();
                            calculator.clear();
                        }
                    }
                }
            }

            if len < PAGE_SIZE {
                break;
            }
            pagenum += 1;
        }

        if stack.len() != 0 {
            insts.push(Inst::Fill(stack));
        }

        insts
    }
}
