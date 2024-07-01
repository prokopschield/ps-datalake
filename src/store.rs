use super::helpers::mapping::{create_ro_mapping, create_rw_mapping, MemoryMapping};
use crate::error::PsDataLakeError;
use crate::helpers::sieve;
use ps_datachunk::Compressor;
use ps_datachunk::DataChunkTrait;
use ps_datachunk::MbufDataChunk;
use ps_datachunk::OwnedDataChunk;
use ps_hash::Hash;
use ps_mbuf::Mbuf;

pub const PTR_SIZE: usize = 4;
pub const CHUNK_SIZE: usize = std::mem::size_of::<DataStorePage>();

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

pub type DataStorePageMbuf<'lt> = Mbuf<'lt, Hash, u8>;

#[repr(C, align(256))]
pub struct DataStorePage<'lt> {
    mbuf: DataStorePageMbuf<'lt>,
}

impl<'lt> DataStorePage<'lt> {
    pub fn mbuf(&'lt self) -> &'lt DataStorePageMbuf {
        &self.mbuf
    }
}

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

    pub fn init(file_path: &'lt str) -> Result<Self, PsDataLakeError> {
        let readonly = false;
        let mapping = Self::load_mapping(file_path, readonly)?;
        let header = Self::get_header(&mapping);

        let total_length = mapping.roref.len();
        let index_length = total_length >> 10;
        let index_offset = ps_datachunk::aligned::rup(std::mem::size_of::<DataStoreHeader>(), 12);
        let data_offset = ps_datachunk::aligned::rup(
            index_offset
                + std::mem::size_of::<Mbuf<(), usize>>()
                + index_length * std::mem::size_of::<usize>(),
            12,
        );
        let data_length = total_length - data_offset;

        header.magic = *b"DataLake\0\0\0\0\0\0\0\0";
        header.index_modulo = sieve::get_le_prime(index_length as u32);
        header.free_chunk = 0;
        header.index_offset = index_offset as u64;
        header.data_offset = data_offset as u64;

        let index: &'lt mut DataStoreIndex = unsafe {
            DataStoreIndex::init_at_ptr(
                mapping.roref.as_ptr().add(index_offset) as *mut u8,
                (),
                index_length,
            )
        };

        let data: &'lt mut DataStorePager = unsafe {
            DataStorePager::init_at_ptr(
                mapping.roref.as_ptr().add(data_offset) as *mut u8,
                (),
                data_length,
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

    pub fn get_chunk_by_index(
        &'lt self,
        index: usize,
    ) -> Result<MbufDataChunk<'lt>, PsDataLakeError> {
        match self.data.get(index) {
            Some(page) => Ok(page.mbuf().into()),
            None => Err(PsDataLakeError::RangeError),
        }
    }

    pub fn calculate_index_bucket(hash: &[u8], index_modulo: u32) -> u32 {
        ps_hash::checksum_u32(hash, hash.len() as u32) % index_modulo
    }

    pub fn get_bucket_index_chunk_by_hash(
        &'lt self,
        hash: &[u8],
    ) -> Result<(u32, u32, Option<MbufDataChunk<'lt>>), PsDataLakeError> {
        let bucket = Self::calculate_index_bucket(hash, self.header.index_modulo);

        for bucket in bucket..self.index.len() as u32 {
            let index = self
                .index
                .get(bucket as usize)
                .ok_or(PsDataLakeError::IndexBucketOverflow)?;

            if *index == 0 {
                return Ok((bucket, 0, None));
            }

            let chunk = self.get_chunk_by_index(*index as usize)?;

            if chunk.hash_ref() == hash {
                return Ok((bucket, *index, Some(chunk)));
            }
        }

        Err(PsDataLakeError::IndexBucketOverflow)
    }

    pub fn get_bucket_by_hash(&'lt self, hash: &[u8]) -> Result<u32, PsDataLakeError> {
        let (bucket, _, _) = self.get_bucket_index_chunk_by_hash(hash)?;

        Ok(bucket)
    }

    pub fn get_index_by_hash(&'lt self, hash: &[u8]) -> Result<u32, PsDataLakeError> {
        let (_, index, chunk) = self.get_bucket_index_chunk_by_hash(hash)?;

        chunk.ok_or(PsDataLakeError::NotFound)?;

        Ok(index)
    }

    pub fn get_chunk_by_hash(
        &'lt self,
        hash: &[u8],
    ) -> Result<MbufDataChunk<'lt>, PsDataLakeError> {
        let (_, _, chunk) = self.get_bucket_index_chunk_by_hash(hash)?;

        Ok(chunk.ok_or(PsDataLakeError::NotFound)?)
    }

    pub fn get_chunk_by_hashkey(
        &'lt self,
        key: &[u8],
        compressor: &Compressor,
    ) -> Result<OwnedDataChunk, PsDataLakeError> {
        if key.len() != 100 {
            let data = ps_base64::decode(key);

            return Ok(OwnedDataChunk::from_data(data));
        }

        let (hash, key) = key.split_at(50);

        let encrypted = self.get_chunk_by_hash(hash)?;
        let decrypted = encrypted.decrypt(key, compressor)?;

        Ok(decrypted)
    }
}
