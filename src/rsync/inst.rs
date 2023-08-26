use std::{
    collections::{HashMap, HashSet},
    io::SeekFrom,
    ops::{Deref, DerefMut},
};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

use crate::fuse::meta::PAGE_SIZE;

#[derive(Deserialize, Serialize, Debug)]
pub enum Inst {
    #[serde(with = "serde_bytes")]
    Fill(Vec<u8>),
    Copy(usize),
}

pub fn byte2page_1based(offset: usize) -> usize {
    (offset + PAGE_SIZE - 1) / PAGE_SIZE
}

#[derive(Deserialize, Serialize, Debug)]
pub struct InstList(Vec<Inst>);

impl InstList {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub async fn corrupt<RW>(
        file: &mut RW,
        relay: &mut HashMap<usize, Vec<u8>>,
        last: usize,
        curr: usize,
    ) where
        RW: AsyncSeekExt + AsyncRead + AsyncWrite + Unpin,
    {
        let mut buf = vec![0; PAGE_SIZE];
        for i in last..curr {
            file.seek(SeekFrom::Start(i as u64)).await.unwrap();
            let len = file.read(&mut buf).await.unwrap();
            if len == PAGE_SIZE {
                relay.insert(i, buf.clone());
            }
        }
    }

    pub async fn recover<MRW, RW>(&self, file: &mut MRW) -> usize
    where
        MRW: AsMut<RW>,
        RW: AsyncSeekExt + AsyncRead + AsyncWrite + Unpin,
    {
        let subfile = file.as_mut();
        let mut buf = vec![0; PAGE_SIZE];
        let mut marked = HashSet::new();

        for inst in &self.0 {
            if let Inst::Copy(offset) = inst {
                marked.insert(*offset);
            }
        }

        let mut relay: HashMap<usize, Vec<u8>> = HashMap::new();
        let mut len = 0;
        for inst in &self.0 {
            let delta = match inst {
                Inst::Fill(bytes) => {
                    subfile.seek(SeekFrom::Start(len as u64)).await.unwrap();
                    subfile.write(&bytes).await.unwrap()
                }

                Inst::Copy(offset) => {
                    if *offset >= byte2page_1based(len) {
                        if *offset * PAGE_SIZE != len {
                            subfile
                                .seek(SeekFrom::Start((*offset * PAGE_SIZE) as u64))
                                .await
                                .unwrap();
                            subfile.read(&mut buf).await.unwrap();

                            subfile.seek(SeekFrom::Start(len as u64)).await.unwrap();
                            subfile.write(&buf).await.unwrap();
                        }
                    } else {
                        subfile.seek(SeekFrom::Start(len as u64)).await.unwrap();
                        subfile.write(relay.get(offset).unwrap()).await.unwrap();
                    }

                    PAGE_SIZE
                }
            };

            Self::corrupt(
                subfile,
                &mut relay,
                byte2page_1based(len),
                byte2page_1based(len + delta),
            )
            .await;

            len += delta;
        }

        len
    }
}

impl Deref for InstList {
    type Target = Vec<Inst>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for InstList {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
