use std::{collections::HashMap, io::SeekFrom};

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::fuse::meta::PAGE_SIZE;

use super::inst::Inst;

pub struct Recovery<'a, F>
where
    F: AsyncSeekExt + AsyncReadExt + AsyncWriteExt + Unpin,
{
    file: &'a mut F,
    buf: Vec<u8>,
    pub len: usize,
    relay: HashMap<usize, Vec<u8>>,
}

pub fn byte2page_1based(offset: usize) -> usize {
    (offset + PAGE_SIZE - 1) / PAGE_SIZE
}

pub fn byte2page_0based(offset: usize) -> usize {
    offset / PAGE_SIZE
}

impl<'a, F> Recovery<'a, F>
where
    F: AsyncSeekExt + AsyncReadExt + AsyncWriteExt + Unpin,
{
    pub fn new<M: AsMut<F>>(file: &'a mut M) -> Self {
        Self {
            file: file.as_mut(),
            buf: vec![0; PAGE_SIZE],
            len: 0,
            relay: HashMap::new(),
        }
    }

    async fn corrupt(&mut self, last: usize, curr: usize) {
        let mut buf = vec![0; PAGE_SIZE];
        for i in last..curr {
            self.file
                .seek(SeekFrom::Start((i * PAGE_SIZE) as u64))
                .await
                .unwrap();
            let len = self.file.read(&mut buf).await.unwrap();
            if len == PAGE_SIZE {
                self.relay.insert(i, buf.clone());
            }
        }
    }

    pub async fn recover(&mut self, inst: Inst) {
        let delta = match &inst {
            Inst::Fill(bytes) => bytes.len(),
            Inst::Copy(_) => PAGE_SIZE,
        };

        // corrupt before overwrited
        self.corrupt(
            byte2page_1based(self.len),
            byte2page_1based(self.len + delta),
        )
        .await;

        match inst {
            Inst::Fill(bytes) => {
                self.file
                    .seek(SeekFrom::Start(self.len as u64))
                    .await
                    .unwrap();
                self.file.write(&bytes).await.unwrap();
            }

            Inst::Copy(offset) => {
                if offset >= byte2page_1based(self.len) {
                    if offset * PAGE_SIZE != self.len {
                        self.file
                            .seek(SeekFrom::Start((offset * PAGE_SIZE) as u64))
                            .await
                            .unwrap();
                        self.file.read(&mut self.buf).await.unwrap();

                        self.file
                            .seek(SeekFrom::Start(self.len as u64))
                            .await
                            .unwrap();
                        self.file.write(&self.buf).await.unwrap();
                    }
                } else {
                    self.file
                        .seek(SeekFrom::Start(self.len as u64))
                        .await
                        .unwrap();
                    self.file
                        .write(self.relay.get(&offset).unwrap())
                        .await
                        .unwrap();
                }
            }
        };

        self.len += delta;
    }
}
