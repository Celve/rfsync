use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq, Hash, Clone, Copy, Deserialize, Serialize, Default, Debug)]
pub struct RollingChecksum(u32);

impl From<u32> for RollingChecksum {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<RollingChecksum> for u32 {
    fn from(value: RollingChecksum) -> Self {
        value.0
    }
}

pub fn compute<T: AsRef<[u8]>>(value: &T) -> RollingChecksum {
    let (mut a, mut b) = (0u16, 0u16);
    let mut bytes = VecDeque::new();
    let len = value.as_ref().len();
    for (i, byte) in value.as_ref().iter().enumerate() {
        bytes.push_back(*byte);
        a = a.wrapping_add(*byte as u16);
        b = b.wrapping_add(((len - i) as u16).wrapping_mul(*byte as u16));
    }
    RollingChecksum::from(((b as u32).wrapping_shl(16)) | (a as u32))
}

pub struct RollingCalculator {
    a: u16,
    b: u16,
    bytes: VecDeque<u8>,
    len: u16,
}

impl RollingCalculator {
    pub fn new(len: u16) -> Self {
        Self {
            a: 0,
            b: 0,
            bytes: VecDeque::new(),
            len,
        }
    }

    pub fn compute(&self) -> Option<RollingChecksum> {
        if self.bytes.len() as u16 == self.len {
            Some(RollingChecksum::from(
                ((self.b as u32).wrapping_shl(16)) | (self.a as u32),
            ))
        } else {
            None
        }
    }

    pub fn forward(&mut self, byte: u8) {
        if self.bytes.len() != self.len as usize {
            self.a = self.a.wrapping_add(byte as u16);
            self.b = self
                .b
                .wrapping_add((self.len - self.bytes.len() as u16).wrapping_mul(byte as u16));

            self.bytes.push_back(byte)
        } else {
            let poped = self.bytes.pop_front().unwrap() as u16;
            self.bytes.push_back(byte);

            self.a = self.a.wrapping_sub(poped).wrapping_add(byte as u16);
            self.b = self
                .b
                .wrapping_sub(self.len.wrapping_mul(poped))
                .wrapping_add(self.a);
        }
    }

    pub fn clear(&mut self) {
        self.a = 0;
        self.b = 0;
        self.bytes.clear();
    }
}
