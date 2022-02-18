use crate::store_file::{InsertInfo, Schema, SchemeArea};
use convenient_skiplist::SkipList;
use lazy_static::lazy_static;
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub struct ContextInfo {
    pub db_name: String,
    pub insert_info: InsertInfo,

    //schema
    pub schema: Schema,
}

lazy_static! {

    ///install dir,TODO using real install dir
    pub static ref INSTALL_DIR:String = String::from("/Users/wanglei/tmp/log/");

    ///bin code serialize string, fixed length of header
    pub static ref BINCODE_STR_FIXED_SIZE: u8= 8;

    pub static ref CONTEXT: Mutex<ContextInfo> = Mutex::new(ContextInfo {
        db_name: String::from(""),
        insert_info: InsertInfo {
            insert_key: String::from(""),
        },
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

pub fn context_set_insert_key(insert_info: &mut InsertInfo, key: String) {
    insert_info.insert_key = key;
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
