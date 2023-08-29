use std::io::SeekFrom;

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::fuse::meta::PAGE_SIZE;

use super::inst::Inst;

pub struct Duplication<'a, R, W>
where
    R: AsyncSeekExt + AsyncReadExt + Unpin,
    W: AsyncSeekExt + AsyncWriteExt + Unpin,
{
    reader: &'a mut R,
    writer: &'a mut W,
    buf: Vec<u8>,
    pub len: usize,
}

impl<'a, R, W> Duplication<'a, R, W>
where
    R: AsyncSeekExt + AsyncReadExt + Unpin,
    W: AsyncSeekExt + AsyncWriteExt + Unpin,
{
    pub fn new<MR: AsMut<R>, MW: AsMut<W>>(reader: &'a mut MR, writer: &'a mut MW) -> Self {
        Self {
            reader: reader.as_mut(),
            writer: writer.as_mut(),
            buf: vec![0; PAGE_SIZE],
            len: 0,
        }
    }

    pub async fn duplicate(&mut self, inst: Inst) {
        let delta = match &inst {
            Inst::Fill(bytes) => bytes.len(),
            Inst::Copy(_) => PAGE_SIZE,
        };

        match inst {
            Inst::Fill(bytes) => {
                self.writer.write(&bytes).await.unwrap();
            }

            Inst::Copy(offset) => {
                self.reader
                    .seek(SeekFrom::Start((offset * PAGE_SIZE) as u64))
                    .await
                    .unwrap();
                self.reader.read(&mut self.buf).await.unwrap();

                self.writer.write(&self.buf).await.unwrap();
            }
        };

        self.len += delta;
    }
}
