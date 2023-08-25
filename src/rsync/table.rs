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

        loop {
            let len = reader.as_mut().read(&mut bytes).await.unwrap();

            for i in 0..len {
                let byte = bytes[i];
                stack.push(byte);
                calculator.forward(byte);

                if let Some(rolling) = calculator.compute() {
                    if let Some(entries) = self.table.get(&rolling) {
                        let md5: Md5 = md5::compute(&stack[(stack.len() - PAGE_SIZE)..]).into();
                        for entry in entries {
                            if entry.md5 == md5 {
                                if stack.len() > PAGE_SIZE {
                                    stack.resize(stack.len() - PAGE_SIZE, 0);
                                    insts.push(Inst::Fill(stack));
                                }
                                let copy_inst = Inst::Copy(entry.offset);
                                insts.push(copy_inst);
                                stack = Vec::new();
                                calculator.clear();
                            }
                        }
                    }
                }
            }

            if len < PAGE_SIZE {
                break;
            }
        }

        if stack.len() != 0 {
            insts.push(Inst::Fill(stack));
        }

        insts
    }
}
