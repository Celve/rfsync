use std::{
    cmp::{max, min, Ordering},
    collections::HashMap,
};

use serde::{Deserialize, Serialize};

/// A vector used to hold the modification/synchronization time.
#[derive(PartialEq, Deserialize, Serialize, Clone, Debug)]
pub struct VecTime {
    /// Map from machine id to modification/synchronization time.
    map: HashMap<usize, usize>,
}

impl VecTime {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn init(mid: usize, time: usize) -> Self {
        let mut map = HashMap::new();
        map.insert(mid, time);
        Self { map }
    }

    /// Extend another `VecTime` with this one, applying the conflict value with filter.
    fn extend(&mut self, other: &Self, filter: impl Fn(usize, usize) -> usize) {
        // self.mappings.extend(other.mappings.iter());
        for (k, v) in other.map.iter() {
            let old_v = self.map.get(k);
            if let Some(old_v) = old_v {
                self.map.insert(*k, filter(*v, *old_v));
            } else {
                self.map.insert(*k, *v);
            }
        }
    }

    /// Merge another `VecTime` with this one, retaining the maximum value when conflicts.
    pub fn merge_max(&mut self, other: &Self) {
        self.extend(other, max);
    }

    /// Merge another `VecTime` with this one, retaining the minimum value when conflicts.
    pub fn merge_min(&mut self, other: &Self) {
        self.extend(other, min);
    }

    // Insert a new mapping to the map.
    pub fn insert(&mut self, mid: usize, time: usize) {
        self.map.insert(mid, time);
    }
}

impl PartialOrd for VecTime {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // not comparable because of different length
        if self.map.len() != other.map.len() {
            return None;
        }

        // less when element-wise less, greater when element-wise greater
        let mut ordering = Ordering::Equal;
        for (k, v) in self.map.iter() {
            let other_v = other.map.get(k);
            if let Some(other_v) = other_v {
                if ordering == Ordering::Equal {
                    ordering = v.cmp(other_v);
                } else if ordering != v.cmp(other_v) {
                    return None;
                }
            } else {
                return None;
            }
        }
        Some(ordering)
    }
}

impl PartialEq<VecTime> for usize {
    fn eq(&self, other: &VecTime) -> bool {
        // expand the `usize` to compare
        other.map.values().all(|v| v == other)
    }
}

impl PartialOrd<VecTime> for usize {
    fn partial_cmp(&self, other: &VecTime) -> Option<Ordering> {
        // expand the `usize` to compare
        let mut ordering = Ordering::Equal;
        for v in other.map.values() {
            if ordering == Ordering::Equal {
                ordering = self.cmp(v)
            } else if ordering != self.cmp(v) {
                return None;
            }
        }
        Some(ordering)
    }
}
