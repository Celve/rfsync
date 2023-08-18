use std::{hash::Hash, num::NonZeroUsize};

use libc::c_int;
use lru::LruCache;

pub struct Alteration<K, const S: usize>
where
    K: Hash + Eq,
{
    free: Vec<usize>,
    lru: LruCache<K, usize>,
}

impl<K, const S: usize> Alteration<K, S>
where
    K: Hash + Eq,
{
    pub fn new() -> Self {
        Self {
            free: (0..S).collect(),
            lru: LruCache::new(NonZeroUsize::new(S).unwrap()),
        }
    }

    pub fn push_lru(&mut self, key: K, id: usize) {
        self.lru.push(key, id);
    }

    pub fn push_free(&mut self, id: usize) {
        self.free.push(id);
    }

    pub fn pop(&mut self) -> Result<usize, c_int> {
        if let Some(bid) = self.free.pop() {
            Ok(bid)
        } else {
            // look for lru
            if let Some((_, bid)) = self.lru.pop_lru() {
                Ok(bid)
            } else {
                Err(libc::ENOBUFS)
            }
        }
    }

    pub fn pop_lru(&mut self, key: &K) {
        self.lru.pop(key);
    }
}
