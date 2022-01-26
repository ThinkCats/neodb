use anyhow::{Context, Result};

use crate::context::{context_schema_info, CONTEXT};
use crate::parse::CreateTableDef;
use std::fs;
use std::fs::{File, OpenOptions};
use std::os::unix::prelude::FileExt;
use std::path::Path;

///startup process
pub fn install_meta_info_store() {
    let context = &CONTEXT.lock().unwrap().schema;
    //TODO cache file fd

    let file = check_or_create_file(context.path.as_str(), context.size).unwrap();
    let free_schema = context.schema_free;
    write_content(
        &file,
        free_schema.offset,
        format!("{}", free_schema.info).as_str(),
    );

    //data offset
    let data_schema = context.schema_data;
    write_content(
        &file,
        data_schema.offset,
        format!("{}", data_schema.info).as_str(),
    );
}

pub fn process_create_db(db: &str) {
    let context = &CONTEXT.lock().unwrap().schema;
    let schema_data = context.schema_data;

    //TODO define protocol
    let tmp = format!("{};", db);
    let db_name = tmp.as_str();
    let data_size = db_name.as_bytes().len() as u64;
    //TODO cache file fd
    let file = check_or_create_file(context.path.as_str(), context.size).unwrap();
    write_content(&file, schema_data.info, db_name);

    //update data offset info and free info
    let data_info = schema_data.info + data_size;
    let free_info = context.schema_free.info - data_size;
    context_schema_info(free_info, data_info)
}

pub fn process_use_db(db_name: &str) {
    //check schema  db exists
    let context = &CONTEXT.lock().unwrap().schema;
    //TODO cache file fd
    let file = check_or_create_file(context.path.as_str(), context.size).unwrap();
    let data_offset = context.schema_data.info;
    let mut buf = vec![0; data_offset as usize];
    read_content(&file, 0, &mut buf);
    println!("read content:{:?}", buf);
}

fn file_exists(path: &str) -> bool {
    Path::new(path).exists()
}

//size unit: MB
pub fn check_or_create_file(path: &str, size: u64) -> Result<File> {
    if file_exists(path) {
        let f = OpenOptions::new()
            .write(true)
            .open(path)
            .context("can not open file")
            .unwrap();
        return Ok(f);
    }
    let f = File::create(path)?;
    f.set_len(size)?;
    Ok(f)
}

pub fn init_table_store(table_create_def: &CreateTableDef) {
    println!(
        "-> Start init table store process, get def:{:?}",
        table_create_def
    );
}

pub fn write_content(f: &File, position: u64, content: &str) -> usize {
    f.write_at(content.as_bytes(), position).unwrap()
}

pub fn read_content(f: &File, position: u64, buf: &mut [u8]) {
    f.read_at(buf, position).unwrap();
}

pub fn delete_file(path: &str) {
    fs::remove_file(path).unwrap();
}
