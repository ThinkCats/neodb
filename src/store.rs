use std::fs::File;
use std::io::Result;
use std::path::Path;

fn file_exists(path: &str) -> bool {
    Path::new(path).exists()
}

pub fn check_or_create_file(path: &str, size: u64) -> Result<()> {
    if file_exists(path) {
        return Ok(());
    }
    let f = File::create(path)?;
    f.set_len(size)?;
    Ok(())
}
