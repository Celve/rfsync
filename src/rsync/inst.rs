use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub enum Inst {
    #[serde(with = "serde_bytes")]
    Fill(Vec<u8>),
    Copy(usize),
}
