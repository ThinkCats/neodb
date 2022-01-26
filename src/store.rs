use anyhow::{Context, Result};

use crate::context::{context_schema_info_update, context_scheme_data_update, CONTEXT};
use crate::parse::CreateTableDef;
use convenient_skiplist::SkipList;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::prelude::FileExt;
use std::path::Path;

///install db process
pub fn install_meta_info_store() {
    let context = &CONTEXT.lock().unwrap().schema;
    //TODO cache file fd

    let mut file = check_or_create_file(context.path.as_str(), context.size).unwrap();
    let free_schema = context.schema_free;
    write_content(
        &mut file,
        free_schema.offset,
        format!("{}", free_schema.info).as_str(),
    );

    //data offset
    let data_schema = context.schema_data;
    write_content(
        &mut file,
        data_schema.offset,
        format!("{}", data_schema.info).as_str(),
    );
}

///load data info in schema file into mem
pub fn startup_load_schema_mem() {
    let context = &mut CONTEXT.lock().unwrap().schema;
    //TODO cache file fd
    let file = check_or_create_file(context.path.as_str(), context.size).unwrap();
    let mut data_offset = context.schema_data.info;
    if data_offset == 0 {
        //maybe init offset, need read from file
        let mut data_offset_buf = vec![0, context.schema_data.capacity];
        let position = context.schema_data.offset;
        read_content(&file, position, &mut data_offset_buf);
        let tmp = String::from_utf8(data_offset_buf).unwrap();
        data_offset = tmp.parse().unwrap();
    }
    println!("read data size:{}", data_offset);
    let mut buf = vec![0; data_offset as usize];
    read_content(&file, 0, &mut buf);
    let read_db = String::from_utf8(buf).unwrap();
    println!("read content:{:?}", read_db);
    let split = read_db
        .split(|x| x == ' ' || x == ';')
        .filter(|x| x.len() > 0)
        .map(|m| m.to_string());
    let skip_list = SkipList::from(split);

    //update mem schema data
    context_scheme_data_update(context, skip_list);
    println!("after startup_load_schema_mem, context:{:?}", context);
}

///create a db
pub fn process_create_db(db: &str) {
    let context = &mut CONTEXT.lock().unwrap().schema;
    let schema_data = context.schema_data;

    //TODO define protocol,eg: protobuf
    let tmp = format!("{};", db);
    let db_name = tmp.as_str();
    let data_size = db_name.as_bytes().len() as u64;
    //TODO cache file fd
    let mut file = check_or_create_file(context.path.as_str(), context.size).unwrap();
    write_content(&mut file, schema_data.info, db_name);

    //update data offset info and free info
    let data_info = schema_data.info + data_size;
    write_content(
        &mut file,
        schema_data.offset,
        format!("{}", data_info).as_str(),
    );

    println!("after startup_load_schema_mem, context:{:?}", context);

    let free_info = context.schema_free.info - data_size;
    context_schema_info_update(context, free_info, data_info);

    //update mem skip_list
    context.data.insert(db_name.to_string());
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

pub fn check_or_create_file(path: &str, size: u64) -> Result<File> {
    if file_exists(path) {
        let f = OpenOptions::new()
            .write(true)
            .read(true)
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

pub fn write_content(f: &mut File, position: u64, content: &str) -> usize {
    let size = f.write_at(content.as_bytes(), position).unwrap();
    f.flush().unwrap();
    return size;
}

pub fn read_content(f: &File, position: u64, buf: &mut [u8]) {
    f.read_at(buf, position).unwrap();
}

pub fn delete_file(path: &str) {
    fs::remove_file(path).unwrap();
}
