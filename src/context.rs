use convenient_skiplist::SkipList;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub struct ContextInfo {
    pub db_name: String,

    //schema
    pub schema: Schema,
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
pub struct ColSchema {
    pub name: String,
    pub col_type: String,
    pub len: u64,
}

impl ColSchema {
    pub const CAP: u16 = 256;
}

#[derive(Debug, Clone)]
pub struct TableDataArea {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct TableData {
    pub schema: Vec<ColSchema>,
    pub data: Vec<TableDataArea>,
}

lazy_static! {

    ///install dir,TODO using real install dir
    pub static ref INSTALL_DIR:String = String::from("/Users/wanglei/tmp/log/");

    ///bin code serialize string, fixed length of header
    pub static ref BINCODE_STR_FIXED_SIZE: u8= 8;

    pub static ref CONTEXT: Mutex<ContextInfo> = Mutex::new(ContextInfo {
        db_name: String::from(""),
        schema: Schema {
            size: 1*1024*1024,
            path: format!("{}{}", INSTALL_DIR.as_str(), String::from("scheme")),
            data: SkipList::new(),
            meta_data: SchemeArea { info: 16, offset: 0 , capacity: 8 },
            meta_free: SchemeArea { info: 1 * 1024*1024 - 8 -8, offset:  8 , capacity: 8 },
        },
    });

}

pub fn context_schema_info_update(schema: &mut Schema, free_info: u64, data_info: u64) {
    schema.meta_data.info = data_info;
    schema.meta_free.info = free_info;
}

pub fn context_scheme_data_update(schema: &mut Schema, data: SkipList<String>) {
    schema.data = data;
}

pub fn context_use_db(db_name: &str) {
    let mut context = CONTEXT.lock().unwrap();
    //check db exists
    let db_existed = context.schema.data.contains(&db_name.to_string());
    if !db_existed {
        println!("[debug] the schema don't existed : {}", db_name);
        return;
    }

    context.db_name = db_name.to_string();
    println!("[debug] after Use, context:{:?}", context);
}
