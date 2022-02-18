use std::mem::size_of_val;

use convenient_skiplist::SkipList;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct InsertInfo {
    pub insert_key: String,
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub size: u64,
    pub data: SkipList<String>,
    pub path: String,

    ///schema area info
    pub meta_data: SchemeArea,
    pub meta_free: SchemeArea,
}

unsafe impl Send for Schema {}

#[derive(Debug, Clone, Copy)]
pub struct SchemeArea {
    pub info: u64,
    pub offset: u64,
    pub capacity: u8,
}

impl Schema {
    pub fn calc_schema_data_offset(&self) {}
}

#[derive(Serialize, Deserialize, PartialEq, PartialOrd, Debug, Clone)]
pub struct TableSchema {
    pub col_schema_list: Vec<ColSchema>,
}

#[derive(Serialize, Deserialize, PartialEq, PartialOrd, Debug, Clone)]
pub struct ColSchema {
    pub name: String,
    pub col_type: String,
    pub len: u64,
}

impl ColSchema {
    pub const CAP: u64 = 256;
    pub const DATA_OFFSET_CAP: u64 = 8;
}

#[derive(Debug, Clone)]
pub struct TableData {
    pub value: String,
    pub free: u64,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataValueEntry {
    pub key: u64,
    pub value: String,
}
