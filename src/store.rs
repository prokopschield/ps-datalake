use super::helpers::mapping::{create_ro_mapping, create_rw_mapping, MemoryMapping};
use crate::error::PsDataLakeError;
use ps_mbuf::Mbuf;

pub const PTR_SIZE: usize = 4;
pub const CHUNK_SIZE: usize = 0x100;

#[repr(C)]
pub struct DataStoreHeader {
    /// the literal `b"DataLake"`
    pub magic: [u8; 16],

    /// the largest prime number < index size
    pub index_modulo: u32,

    /// the next usable offset
    pub free_chunk: u32,

    /// byte offset of the index
    pub index_offset: u64,

    /// byte offset of chunks
    pub data_offset: u64,
}

pub type DataStorePage<'lt> = Mbuf<'lt, [u8; 50], u8>;
pub type DataStoreIndex<'lt> = Mbuf<'lt, (), u32>;
pub type DataStorePager<'lt> = Mbuf<'lt, (), DataStorePage<'lt>>;

pub struct DataStore<'lt> {
    pub header: &'lt mut DataStoreHeader,
    pub index: &'lt mut DataStoreIndex<'lt>,
    pub data: &'lt mut DataStorePager<'lt>,
    pub mapping: MemoryMapping<'lt>,
    pub readonly: bool,
}

impl<'lt> DataStore<'lt> {
    pub fn load_mapping(
        file_path: &'lt str,
        readonly: bool,
    ) -> Result<MemoryMapping<'lt>, PsDataLakeError> {
        let mapping: MemoryMapping<'lt> = if readonly {
            create_ro_mapping(file_path)?
        } else {
            create_rw_mapping(file_path)?
        };

        Ok(mapping)
    }

    pub fn get_header(mapping: &MemoryMapping<'lt>) -> &'lt mut DataStoreHeader {
        unsafe { &mut *(mapping.roref.as_ptr() as *mut DataStoreHeader) }
    }

    pub fn load(file_path: &'lt str, readonly: bool) -> Result<Self, PsDataLakeError> {
        let mapping = Self::load_mapping(file_path, readonly)?;

        let header = Self::get_header(&mapping);

        let index: &'lt mut DataStoreIndex = unsafe {
            DataStoreIndex::at_offset_mut(
                mapping.roref.as_ptr() as *mut u8,
                header.index_offset as usize,
            )
        };

        let data: &'lt mut DataStorePager = unsafe {
            DataStorePager::at_offset_mut(
                mapping.roref.as_ptr() as *mut u8,
                header.data_offset as usize,
            )
        };

        Ok(Self {
            header,
            index,
            data,
            mapping,
            readonly,
        })
    }
}
