use crate::parse::CreateTableDef;
use std::fs::File;
use std::io::Result;
use std::path::Path;

fn file_exists(path: &str) -> bool {
    Path::new(path).exists()
}

//size unit: MB
pub fn check_or_create_file(path: &str, size: u64) -> Result<()> {
    if file_exists(path) {
        return Ok(());
    }
    let f = File::create(path)?;
    f.set_len(size * 1024 * 1024)?;
    Ok(())
}

//TODO add init store process
pub fn init_meta_store() {}

pub fn init_table_store(table_create_def: CreateTableDef) {}
