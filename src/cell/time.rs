use std::{cmp::Ordering, collections::HashMap};

use serde::{Deserialize, Serialize};

/// A vector used to hold the modification/synchronization time.
#[derive(PartialEq, Deserialize, Serialize, Clone, Debug, Default)]
pub struct VecTime {
    /// Map from machine id to modification/synchronization time.
    map: HashMap<u64, u64>,
}

impl VecTime {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Extend another `VecTime` with this one, applying the conflict value with filter.
    pub fn union(&mut self, other: &Self, filter: impl Fn(u64, u64) -> u64) {
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

    pub fn intersect(&mut self, other: &Self, filter: impl Fn(u64, u64) -> u64) {
        self.map = self
            .map
            .iter()
            .filter_map(|(k, v1)| other.map.get(k).map(|v2| (*k, filter(*v1, *v2))))
            .collect();
    }

    // Insert a new mapping to the map.
    pub fn insert(&mut self, mid: u64, time: u64) {
        self.map.insert(mid, time);
    }

    pub fn get(&mut self, mid: u64) -> Option<u64> {
        self.map.get(&mid).map(|v| *v)
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl PartialOrd for VecTime {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // less when element-wise less, greater when element-wise greater
        let keys = self
            .map
            .keys()
            .chain(other.map.keys())
            .cloned()
            .collect::<Vec<_>>();

        let mut ordering = Ordering::Equal;
        for k in keys {
            let self_v = self.map.get(&k);
            let other_v = other.map.get(&k);
            let res = match (self_v, other_v) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (Some(self_v), Some(other_v)) => self_v.cmp(other_v),
            };

            if res != Ordering::Equal {
                if ordering != Ordering::Equal && ordering != res {
                    return None;
                } else {
                    ordering = res;
                }
            }
        }

        Some(ordering)
    }
}

impl PartialEq<VecTime> for u64 {
    fn eq(&self, other: &VecTime) -> bool {
        if other.is_empty() {
            // pay attention to the empty situation
            false
        } else {
            // expand the `usize` to compare
            other.map.values().all(|v| v == other)
        }
    }
}

impl PartialOrd<VecTime> for u64 {
    fn partial_cmp(&self, other: &VecTime) -> Option<Ordering> {
        if other.is_empty() {
            // pay attention to the empty situation
            None
        } else {
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
}

impl From<&VecTime> for HashMap<u64, u64> {
    fn from(value: &VecTime) -> Self {
        value.map.clone()
    }
}

impl From<&HashMap<u64, u64>> for VecTime {
    fn from(value: &HashMap<u64, u64>) -> Self {
        Self {
            map: value.into_iter().map(|(k, v)| (*k, *v)).collect(),
        }
    }
}
