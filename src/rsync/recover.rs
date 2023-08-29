use std::io::SeekFrom;

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::{
    buffer::disk::DiskManager, disk::direct::PrefixDirectDiskManager, fuse::meta::PAGE_SIZE,
};

use super::inst::Inst;

pub fn byte2page_1based(offset: usize) -> usize {
    (offset + PAGE_SIZE - 1) / PAGE_SIZE
}

pub fn byte2page_0based(offset: usize) -> usize {
    offset / PAGE_SIZE
}

pub struct Recovery<'a, F, D>
where
    F: AsyncSeekExt + AsyncReadExt + AsyncWriteExt + Unpin,
    D: DiskManager<usize, Vec<u8>>,
{
    file: &'a mut F,
    dm: D,
    buf: Vec<u8>,
    pub len: usize,
}

pub type RecoverDiskManager = PrefixDirectDiskManager<usize, Vec<u8>>;

impl<'a, F, D> Recovery<'a, F, D>
where
    F: AsyncSeekExt + AsyncReadExt + AsyncWriteExt + Unpin,
    D: DiskManager<usize, Vec<u8>>,
{
    pub fn new<M: AsMut<F>>(file: &'a mut M, dm: D) -> Self {
        Self {
            file: file.as_mut(),
            dm,
            buf: vec![0; PAGE_SIZE],
            len: 0,
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
                self.dm.write(&i, &buf).await;
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
                let offset = offset as usize;
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
                    self.file.write(&self.dm.read(&offset).await).await.unwrap();
                }
            }
        };

        self.len += delta;
    }
}
