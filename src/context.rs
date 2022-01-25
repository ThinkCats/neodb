use lazy_static::lazy_static;
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub struct ContextInfo {
    pub db_name: String,

    //schema
    pub schema_size: u64,
    pub schema_path: String,
    pub schema_free_size: u8,
    pub schema_offset_size: u8,
}

#[derive(Debug, Clone)]
pub struct SchemaContext {}

lazy_static! {

    ///install dir,TODO using real install dir
    static ref INSTALL_DIR:String = String::from("/Users/wanglei/tmp/log/");

    ///scheme file

    pub static ref CONTEXT: Mutex<ContextInfo> = Mutex::new(ContextInfo {
        db_name: String::from(""),
        schema_size: 1 * 1024 * 1024,
        schema_path: format!("{}{}", INSTALL_DIR.as_str(), String::from("scheme")),
        schema_offset_size:8,
        schema_free_size:8,
    });

}

pub fn context_use_db(db_name: &str) {
    CONTEXT.lock().unwrap().db_name = db_name.to_string();
}
