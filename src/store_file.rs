use std::mem::size_of_val;

#[derive(Debug, Clone)]
pub struct SSDataEntry {
    pub key: i64,
    pub value: String,
}

impl PartialOrd for SSDataEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.key.cmp(&other.key))
    }
}

impl PartialEq for SSDataEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.value == other.value
    }
}

impl SSDataEntry {
    pub fn length(&self) -> usize {
        size_of_val(&self.key) + self.value.len()
    }
}

pub struct SSFilter {
    //TODO bloom filter
}

pub struct SSBasic {
    free: u64,
    last_offset: u64,
}

pub struct SSTable {
    data_block: Vec<SSDataEntry>,
}
