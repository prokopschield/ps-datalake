use crate::error::Result;
use crate::store::MAGIC;
use std::fs::File;
use std::io::Read;

pub fn read_magic(path: &str) -> Result<[u8; 16]> {
    let mut file = File::open(path)?;
    let mut buffer = [0; 16];

    file.read_exact(&mut buffer)?;

    Ok(buffer)
}

pub fn verify_magic(path: &str) -> Result<bool> {
    Ok(read_magic(path)? == MAGIC)
}
