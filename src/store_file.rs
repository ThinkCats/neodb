use std::mem::size_of_val;

use serde::{Deserialize, Serialize};

pub struct Basic {
    free: u64,
    last_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataIdxEntry {
    pub key: u64,
    pub offset: u64,
}

impl PartialOrd for DataIdxEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.key.cmp(&other.key))
    }
}

impl PartialEq for DataIdxEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.offset == other.offset
    }
}

impl DataIdxEntry {
    pub fn length(&self) -> usize {
        size_of_val(&self.key) + size_of_val(&self.offset)
    }
}

pub struct SSFilter {
    //TODO bloom filter
}

pub struct SSTable {
    data_block: Vec<DataIdxEntry>,
}
