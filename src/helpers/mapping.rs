use std::{
    error::Error,
    sync::{Arc, Mutex},
};

pub struct MemoryMapping {
    pub owned_ro: Option<Arc<Mutex<memmap::Mmap>>>,
    pub owned_rw: Option<Arc<Mutex<memmap::MmapMut>>>,
    pub roref: &'static [u8],
}

impl MemoryMapping {
    pub fn get_ro_slice(&self, offset: usize, length: usize) -> &[u8] {
        &self.roref[offset..offset + length]
    }

    pub fn read_u32(&self, offset_in_u32_chunks: u32) -> u32 {
        let offset = offset_in_u32_chunks << 2;
        let bytes = self.get_ro_slice(offset as usize, 4);

        u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
    }
}

pub fn create_ro_mapping(file_path: &str) -> Result<MemoryMapping, Box<dyn Error>> {
    let file = std::fs::OpenOptions::new().read(true).open(file_path)?;

    let mmap = unsafe { memmap::MmapOptions::new().map(&file)? };

    let slice: &[u8] = unsafe { std::slice::from_raw_parts(mmap[..].as_ptr(), mmap.len()) };

    let arc = Arc::from(Mutex::from(mmap));

    return Ok(MemoryMapping {
        owned_ro: Some(arc),
        owned_rw: None,
        roref: slice,
    });
}

pub fn create_rw_mapping(file_path: &str) -> Result<MemoryMapping, Box<dyn Error>> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(file_path)?;

    let mmap = unsafe { memmap::MmapOptions::new().map(&file)? };

    let mmut = mmap.make_mut()?;

    let slice: &[u8] = unsafe { std::slice::from_raw_parts(mmut[..].as_ptr(), mmut.len()) };

    let mutex = Mutex::from(mmut);
    let arc = Arc::from(mutex);

    Ok(MemoryMapping {
        owned_ro: None,
        owned_rw: Some(arc),
        roref: slice,
    })
}
