use serde::{Deserialize, Serialize};

use crate::rpc::{fake_inst::FillOrCopy, FakeInst};

#[derive(Deserialize, Serialize, Debug)]
pub enum Inst {
    #[serde(with = "serde_bytes")]
    Fill(Vec<u8>),
    Copy(u64),
}

impl From<Inst> for FakeInst {
    fn from(value: Inst) -> Self {
        let fill_or_copy = Some(match value {
            Inst::Fill(data) => FillOrCopy::Data(data),
            Inst::Copy(offset) => FillOrCopy::Offset(offset),
        });

        Self { fill_or_copy }
    }
}

impl From<FakeInst> for Inst {
    fn from(value: FakeInst) -> Self {
        let fill_or_copy = value.fill_or_copy.unwrap();
        match fill_or_copy {
            FillOrCopy::Data(data) => Self::Fill(data),
            FillOrCopy::Offset(offset) => Self::Copy(offset),
        }
    }
}
