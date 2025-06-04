use crate::error::Result;
use crate::store::MAGIC;
use std::fs::File;
use std::io::Read;
use std::path::Path;

pub fn read_magic<P>(path: P) -> Result<[u8; 16]>
where
    P: AsRef<Path>,
{
    let mut file = File::open(path)?;
    let mut buffer = [0; 16];

    file.read_exact(&mut buffer)?;

    Ok(buffer)
}

pub fn verify_magic<P>(path: P) -> Result<bool>
where
    P: AsRef<Path>,
{
    Ok(read_magic(path)? == MAGIC)
}
