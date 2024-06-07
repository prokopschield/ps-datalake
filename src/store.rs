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
    pub index_modulo: usize,

    /// the next usable offset
    pub free_chunk: usize,

    /// byte offset of the index
    pub index_offset: usize,

    /// byte offset of chunks
    pub data_offset: usize,
}

pub struct DataStore<'lt> {
    pub header: &'lt mut DataStoreHeader,
    pub index: &'lt mut Mbuf<'lt, (), usize>,
    pub data: &'lt mut Mbuf<'lt, [u8; 50], u8>,
    pub mapping: MemoryMapping<'lt>,
    pub readonly: bool,
}

impl<'lt> DataStore<'lt> {
    pub fn load(file_path: &'lt str, readonly: bool) -> Result<Self, PsDataLakeError> {
        let mapping: MemoryMapping<'lt> = if readonly {
            create_ro_mapping(file_path)?
        } else {
            create_rw_mapping(file_path)?
        };

        let header = unsafe { &mut *(mapping.roref.as_ptr() as *mut DataStoreHeader) };

        let index = unsafe {
            Mbuf::<'lt, (), usize>::at_offset_mut(
                mapping.roref.as_ptr() as *mut u8,
                header.index_offset,
            )
        };

        let data = unsafe {
            Mbuf::<'lt, [u8; 50], u8>::at_offset_mut(
                mapping.roref.as_ptr() as *mut u8,
                header.data_offset,
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
