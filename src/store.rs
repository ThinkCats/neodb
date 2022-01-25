use crate::parse::CreateTableDef;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Result;
use std::os::unix::prelude::FileExt;
use std::path::Path;

fn file_exists(path: &str) -> bool {
    Path::new(path).exists()
}

//size unit: MB
pub fn check_or_create_file(path: &str, size: u64) -> Result<File> {
    if file_exists(path) {
        let f = OpenOptions::new().write(true).open(path).unwrap();
        return Ok(f);
    }
    let f = File::create(path)?;
    f.set_len(size * 1024 * 1024)?;
    Ok(f)
}

//TODO add init store process
pub fn init_meta_store() {}

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
