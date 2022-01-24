pub struct SSDataEntry {
    key: String,
    value: String,
    length: u64,
}

pub struct SSFilter {
    //TODO bloom filter
}

pub struct SSTable {
    data_block: Vec<SSDataEntry>,
}
