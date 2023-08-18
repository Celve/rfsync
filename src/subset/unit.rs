use std::{fmt::Display, hash::Hash};

use tokio::sync::{Mutex, RwLock};

pub struct SubsetUnit<K>
where
    K: Default + Clone + Copy + Display + Hash + Eq + Send + Sync + 'static,
{
    pub(super) bid: usize,

    /// `key` could only be reset with the reset function.
    pub(super) key: RwLock<K>,

    /// The `refcnt` should not be replaced by `AtomicUsize`,
    /// because critical section is required when increaing or decreasing it.
    pub(super) refcnt: Mutex<usize>,
}

impl<K> SubsetUnit<K>
where
    K: Default + Clone + Copy + Display + Hash + Eq + Send + Sync + 'static,
{
    pub fn new(bid: usize) -> Self {
        Self {
            bid,
            key: RwLock::new(Default::default()),
            refcnt: Mutex::new(0),
        }
    }
}
