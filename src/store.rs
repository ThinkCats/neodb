use anyhow::{Context, Result};

use crate::context::{
    context_schema_info_update, context_scheme_data_update, BINCODE_STR_FIXED_SIZE, CONTEXT,
};
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
    let free_schema = context.meta_free;
    write_content(
        &mut file,
        free_schema.offset,
        &bincode::serialize(&free_schema.info).unwrap(),
    );

    //data offset
    let data_schema = context.meta_data;
    write_content(
        &mut file,
        data_schema.offset,
        &bincode::serialize(&data_schema.info).unwrap(),
    );
}

///load data info in schema file into mem
pub fn startup_load_schema_mem() {
    let context = &mut CONTEXT.lock().unwrap().schema;
    //TODO cache file fd
    let file = check_or_create_file(context.path.as_str(), context.size).unwrap();
    let init_offset = context.meta_data.info;
    println!("[debug] read data size:{}", init_offset);

    //read meta data
    let meta_data_cap = context.meta_data.capacity as usize;
    let meta_free_cap = context.meta_free.capacity as usize;
    let meta_total_cap = meta_data_cap + meta_free_cap;
    let mut meta_buf = vec![0; meta_total_cap];
    read_content(&file, 0, &mut meta_buf);
    let meta_data_buf = meta_buf.get(0..meta_data_cap).unwrap();
    let meta_free_buf = meta_buf.get(meta_data_cap..meta_total_cap).unwrap();
    let meta_data: u64 = bincode::deserialize(&meta_data_buf).unwrap();
    let meta_free: u64 = bincode::deserialize(&meta_free_buf).unwrap();
    println!(
        "[debug] read meta content:{:?}, data:{:?},free:{:?}",
        meta_buf, meta_data, meta_free
    );

    let schema_data_size = meta_data - meta_total_cap as u64;
    if schema_data_size == 0 {
        //schema data is blank
        return;
    }

    //update meme schema meta info
    context_schema_info_update(context, meta_free, meta_data);

    //read schema info
    let mut schema_buf = vec![0u8; schema_data_size as usize];
    let schema_start_offset = meta_total_cap as u64;
    read_content(&file, schema_start_offset, &mut schema_buf);
    println!("[debug] read content buf:{:?}", schema_buf);

    //let read_db: String = bincode::deserialize(&schema_buf).unwrap();
    let read_db = iter_buf(&mut schema_buf);
    println!("[debug] read content:{:?}", read_db);
    let mut skip_list = SkipList::new();
    for v in read_db {
        skip_list.insert(v);
    }
    //update mem schema data
    context_scheme_data_update(context, skip_list);
    println!(
        "[debug] after startup_load_schema_mem, context:{:?}",
        context
    );
}

pub fn iter_buf(buf: &mut [u8]) -> Vec<String> {
    //get fixed header
    let mut result: Vec<String> = Vec::new();
    let mut offset = 0;
    loop {
        if offset >= buf.len() {
            break;
        }

        let fixed_len = *BINCODE_STR_FIXED_SIZE as usize;
        let ele_lenth_buf = buf.get(offset..offset + fixed_len).unwrap();
        let ele_len: u64 = bincode::deserialize(ele_lenth_buf).unwrap();
        let ele_end_offset = fixed_len + offset + ele_len as usize;
        let ele_buf = buf.get(offset..ele_end_offset).unwrap();
        let content = bincode::deserialize(ele_buf).unwrap();

        result.push(content);
        offset = ele_end_offset;
    }
    return result;
}

///create a db
pub fn process_create_db(db_name: &str) {
    let context = &mut CONTEXT.lock().unwrap().schema;

    //check db exists
    if context.data.contains(&db_name.to_string()) {
        println!("schema has existed, abort.");
        return;
    }

    let meta_data = context.meta_data;
    let data_size = db_name.as_bytes().len() as u64;

    //TODO cache file fd
    let mut file = check_or_create_file(context.path.as_str(), context.size).unwrap();
    println!(
        "[debug] create db write file, position:{}, name:{}",
        meta_data.info, db_name
    );

    write_content(
        &mut file,
        meta_data.info,
        &bincode::serialize(&db_name).unwrap(),
    );

    //update data offset info and free info
    //because of bincode serialize, length add 8
    let data_info = meta_data.info + data_size + *BINCODE_STR_FIXED_SIZE as u64;

    println!(
        "[debug] create db write file data offset, position:{}, name:{}",
        meta_data.offset,
        format!("{}", data_info)
    );

    write_content(
        &mut file,
        meta_data.offset,
        &bincode::serialize(&data_info).unwrap(),
    );

    let free_info = context.meta_free.info - data_size;
    write_content(
        &mut file,
        context.meta_free.offset,
        &bincode::serialize(&free_info).unwrap(),
    );

    context_schema_info_update(context, free_info, data_info);

    //update mem skip_list, db not end with ;
    context.data.insert(db_name.to_string());

    println!("[debug] after insert skip_list , context:{:?}", context);
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
        "[debug] Start init table store process, get def:{:?}",
        table_create_def
    );
}

pub fn write_content(f: &mut File, position: u64, content: &[u8]) -> usize {
    let size = f.write_at(content, position).unwrap();
    f.flush().unwrap();
    return size;
}

pub fn read_content(f: &File, position: u64, buf: &mut [u8]) {
    f.read_at(buf, position).unwrap();
}

pub fn delete_file(path: &str) {
    fs::remove_file(path).unwrap();
}
