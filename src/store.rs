use anyhow::{Context, Result};

use crate::context::CONTEXT;
use crate::parse::CreateTableDef;
use std::fs;
use std::fs::{File, OpenOptions};
use std::os::unix::prelude::FileExt;
use std::path::Path;

//TODO add init store process
pub fn init_meta_store() {
    let context = CONTEXT.lock().unwrap();
    //TODO cache file fd
    let scheme_size = context.schema_size;
    let file = check_or_create_file(context.schema_path.as_str(), scheme_size).unwrap();

    //write free offset info
    let free_offset = scheme_size - u64::from(context.schema_free_size);

    //actual use size
    let use_size = context.schema_free_size + context.schema_offset_size;
    //if scheme_size < use_size as u64 {
    //    panic!("system error,can not cal free size, because free < 0 ");
    //}
    let free = scheme_size - u64::from(use_size);
    println!("free offsize:{}, size:{}", free_offset, free);
    write_content(&file, free_offset, format!("{}", free).as_str());

    //data offset
    let data_offset = scheme_size - u64::from(use_size);
    println!("data offsize:{}", data_offset);
    write_content(&file, data_offset, "0");
}

pub fn process_use_db(db_name: &str) {
    //check schema  db exists
    let context = CONTEXT.lock().unwrap();
    //TODO cache file fd
    let file = check_or_create_file(context.schema_path.as_str(), context.schema_size).unwrap();
    //TODO read info from end of file
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
    //f.seek(SeekFrom::Current(position)).unwrap();
    f.write_at(content.as_bytes(), position).unwrap()
}

pub fn delete_file(path: &str) {
    fs::remove_file(path).unwrap();
}
