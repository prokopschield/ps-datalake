use crate::error::PsDataLakeError;
use crate::helpers::sieve;
use ps_datachunk::Compressor;
use ps_datachunk::DataChunkTrait;
use ps_datachunk::MbufDataChunk;
use ps_datachunk::OwnedDataChunk;
use ps_hash::Hash;
use ps_mbuf::Mbuf;
use ps_mmap::MemoryMapping;
use ps_mmap::MmapOptions;
use std::sync::Arc;
use std::sync::Mutex;

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
    pub fn bytes_to_pages(bytes: usize) -> usize {
        (bytes + std::mem::size_of::<DataStorePageMbuf>()).div_ceil(CHUNK_SIZE)
    }
}

pub type DataStoreIndex<'lt> = Mbuf<'lt, (), u32>;
pub type DataStorePager<'lt> = Mbuf<'lt, (), DataStorePage<'lt>>;

pub struct DataStoreShared<'lt> {
    pub header: &'lt DataStoreHeader,
    pub index: &'lt DataStoreIndex<'lt>,
    pub pager: &'lt DataStorePager<'lt>,
}

pub struct DataStoreAtomic<'lt> {
    pub header: &'lt mut DataStoreHeader,
    pub index: &'lt mut DataStoreIndex<'lt>,
    pub pager: &'lt mut DataStorePager<'lt>,
    pub mapping: MemoryMapping<'lt>,
}

pub struct DataStore<'lt> {
    pub shared: DataStoreShared<'lt>,
    pub atomic: Arc<Mutex<DataStoreAtomic<'lt>>>,
    pub readonly: bool,
}

impl<'lt> DataStore<'lt> {
    pub fn load_mapping(
        file_path: &'lt str,
        readonly: bool,
    ) -> Result<MemoryMapping<'lt>, PsDataLakeError> {
        let mapping = MemoryMapping::new_backed(&MmapOptions::new(), file_path, readonly)?;

        Ok(mapping)
    }

    pub unsafe fn get_header(mapping: &MemoryMapping<'lt>) -> &'lt mut DataStoreHeader {
        &mut *(mapping.as_ptr() as *mut DataStoreHeader)
    }

    pub unsafe fn get_index(mapping: &MemoryMapping<'lt>) -> &'lt mut DataStoreIndex<'lt> {
        DataStoreIndex::at_offset_mut(
            mapping.as_ptr() as *mut u8,
            Self::get_header(mapping).index_offset as usize,
        )
    }

    pub unsafe fn get_pager(mapping: &MemoryMapping<'lt>) -> &'lt mut DataStorePager<'lt> {
        DataStorePager::at_offset_mut(
            mapping.as_ptr() as *mut u8,
            Self::get_header(mapping).data_offset as usize,
        )
    }

    pub fn load(file_path: &'lt str, readonly: bool) -> Result<Self, PsDataLakeError> {
        let mapping = Self::load_mapping(file_path, readonly)?;

        let shared = DataStoreShared {
            header: unsafe { Self::get_header(&mapping) },
            index: unsafe { Self::get_index(&mapping) },
            pager: unsafe { Self::get_pager(&mapping) },
        };

        let atomic = DataStoreAtomic {
            header: unsafe { Self::get_header(&mapping) },
            index: unsafe { Self::get_index(&mapping) },
            pager: unsafe { Self::get_pager(&mapping) },
            mapping,
        };

        let atomic = Mutex::from(atomic);
        let atomic = Arc::from(atomic);

        let store = DataStore {
            shared,
            atomic,
            readonly,
        };

        Ok(store)
    }

    pub fn init(file_path: &'lt str) -> Result<Self, PsDataLakeError> {
        let readonly = false;
        let mapping = Self::load_mapping(file_path, readonly)?;
        let header = unsafe { Self::get_header(&mapping) };

        let total_length = mapping.len();
        let index_length = total_length >> 10;
        let index_modulo = sieve::get_le_prime(index_length as u32);
        let index_offset = ps_datachunk::aligned::rup(std::mem::size_of::<DataStoreHeader>(), 12);
        let data_offset = ps_datachunk::aligned::rup(
            index_offset
                + std::mem::size_of::<Mbuf<(), usize>>()
                + index_length * std::mem::size_of::<usize>(),
            12,
        );

        let arc = mapping.rw()?;
        let mut map = arc.lock()?;

        unsafe {
            DataStoreIndex::init_at_ptr(
                (&mut map[index_offset..index_offset]).as_mut_ptr(),
                (),
                index_length,
            );
        }

        unsafe {
            DataStorePager::init_at_ptr(
                (&mut map[data_offset..data_offset]).as_mut_ptr(),
                (),
                (total_length - data_offset) / CHUNK_SIZE,
            );
        }

        header.magic = *b"DataLake\0\0\0\0\0\0\0\0";
        header.index_modulo = index_modulo;
        header.free_chunk = 0;
        header.index_offset = index_offset as u64;
        header.data_offset = data_offset as u64;

        Self::load(file_path, false)
    }

    pub fn get_chunk_by_index(
        &'lt self,
        index: usize,
    ) -> Result<MbufDataChunk<'lt>, PsDataLakeError> {
        match self.shared.pager.get(index) {
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
        let bucket = Self::calculate_index_bucket(hash, self.shared.header.index_modulo);

        for bucket in bucket..self.shared.index.len() as u32 {
            let index = self
                .shared
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
