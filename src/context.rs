use lazy_static::lazy_static;
use std::sync::Mutex;

#[derive(Debug)]
pub struct ContextInfo {
    pub db_name: String,
}

lazy_static! {
    pub static ref CONTEXT: Mutex<ContextInfo> = Mutex::new(ContextInfo {
        db_name: String::from("")
    });
}

pub fn use_db(db_name: &str) {
    CONTEXT.lock().unwrap().db_name = db_name.to_string();
}
