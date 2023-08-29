use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};

use crate::{
    fuse::meta::PAGE_SIZE,
    rpc::iter::Iterator,
    rsync::{inst::Inst, roll::RollingCalculator},
};

use super::{hashed::Md5, table::HashTable};

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
    pub fn new<M>(table: &'a HashTable, reader: &'a mut M) -> Self
    where
        M: AsMut<F>,
    {
        Self {
            table,
            reader: reader.as_mut(),
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
    type Item = Vec<Inst>;

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
                let mut insts = Vec::new();
                insts.push(Inst::Fill(self.stack.clone()));
                Some(insts)
            };
        }

        let mut insts = Vec::new();
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
                        insts.push(Inst::Copy(offset as u64));
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
