use super::helpers::mapping::{create_ro_mapping, create_rw_mapping, MemoryMapping};
use std::{
    error::Error,
    sync::{Arc, Mutex},
};

pub const PTR_SIZE: usize = 4;
pub const CHUNK_SIZE: usize = 0x100;

#[repr(C)]
pub struct DataStoreHeader {
    pub magic: [u8; 16],

    pub index_modulo: u32,
    pub free_chunk: u32,

    pub reserved_1: u64,

    pub file_size_bytes: u64,
    pub file_size_ptrs: u64,
    pub file_size_chunks: u64,
    pub reserved_2: u64,

    pub index_size_bytes: u64,
    pub index_size_ptrs: u64,
    pub index_size_chunks: u64,
    pub reserved_3: u64,

    pub index_offset_bytes: u64,
    pub index_offset_ptrs: u64,
    pub index_offset_chunks: u64,
    pub reserved_4: u64,

    pub data_size_bytes: u64,
    pub data_size_ptrs: u64,
    pub data_size_chunks: u64,
    pub reserved_5: u64,

    pub data_offset_bytes: u64,
    pub data_offset_ptrs: u64,
    pub data_offset_chunks: u64,
    pub reserved_6: u64,
}

#[repr(C)]
pub struct DataChunkOuterHeader {
    pub hash: [u8; 50],
    pub packed_size: u32,
}

#[repr(C)]
pub struct DataChunkInnerHeader {
    pub hash: [u8; 50],
    pub raw_size: u32,
}

pub struct DataStore {
    pub header: Arc<Mutex<&'static mut DataStoreHeader>>,
    pub roheader: &'static DataStoreHeader,
    pub index: &'static mut [u32],
    pub dataptr: *mut u8,
    mapping: MemoryMapping,
    readonly: bool,
}

impl DataStore {
    pub fn load(file_path: &str, readonly: bool) -> Result<Self, Box<dyn Error>> {
        let mapping = if readonly {
            create_ro_mapping(file_path)?
        } else {
            create_rw_mapping(file_path)?
        };

        let roheader = unsafe { &*(mapping.roref.as_ptr() as *const DataStoreHeader) };

        let header = Arc::from(Mutex::from(unsafe {
            &mut *(mapping.roref.as_ptr() as *mut DataStoreHeader)
        }));

        let index = unsafe {
            std::slice::from_raw_parts_mut(
                &mut *((mapping
                    .roref
                    .as_ptr()
                    .add(roheader.index_offset_bytes as usize)) as *mut u32),
                roheader.index_size_chunks as usize,
            )
        };

        Ok(Self {
            header,
            roheader,
            index,
            dataptr: mapping.roref.as_ptr() as *mut u8,
            mapping,
            readonly,
        })
    }
}
