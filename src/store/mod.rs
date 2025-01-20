mod atomic;
mod shared;

use std::marker::PhantomData;

use crate::error::PsDataLakeError;
use crate::error::Result;
use crate::helpers::sieve;
use atomic::DataStoreWriteGuard;
use ps_datachunk::utils::round_up;
use ps_datachunk::BorrowedDataChunk;
use ps_datachunk::DataChunk;
use ps_datachunk::DataChunkTrait;
use ps_datachunk::MbufDataChunk;
use ps_datachunk::OwnedDataChunk;
use ps_hash::Hash;
use ps_hkey::Hkey;
use ps_hkey::LongHkeyExpanded;
use ps_mbuf::Mbuf;
use ps_mmap::MemoryMap;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use shared::DataStoreReadGuard;

pub const MAGIC: [u8; 16] = *b"DataLake\0\0\0\0\0\0\0\0";
pub const PTR_SIZE: usize = 4;
pub const CHUNK_SIZE: usize = std::mem::size_of::<DataStorePage>();
pub const DATA_CHUNK_MIN_SIZE: usize = 75;
pub const DATA_CHUNK_MAX_RAW_SIZE: usize = 4096;
pub const DATA_CHUNK_MAX_ENCRYPTED_SIZE: usize = 4117;

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

#[derive(Clone)]
pub struct DataStore<'lt> {
    mmap: MemoryMap,
    readonly: bool,
    _data: PhantomData<&'lt [u8]>,
}

impl<'lt> DataStore<'lt> {
    pub fn load_mapping(file_path: &str, readonly: bool) -> Result<MemoryMap> {
        Ok(MemoryMap::map(file_path, readonly)?)
    }

    fn shared(&self) -> DataStoreReadGuard<'lt> {
        self.into()
    }

    fn atomic(&self) -> Result<DataStoreWriteGuard> {
        Ok(self.try_into()?)
    }

    pub unsafe fn get_header(mapping: &MemoryMap) -> &'lt mut DataStoreHeader {
        &mut *(mapping.as_ptr() as *mut DataStoreHeader)
    }

    pub unsafe fn get_index(mapping: &MemoryMap) -> &'lt mut DataStoreIndex<'lt> {
        DataStoreIndex::at_offset_mut(
            mapping.as_ptr() as *mut u8,
            Self::get_header(mapping).index_offset as usize,
        )
    }

    pub unsafe fn get_pager(mapping: &MemoryMap) -> &'lt mut DataStorePager<'lt> {
        DataStorePager::at_offset_mut(
            mapping.as_ptr() as *mut u8,
            Self::get_header(mapping).data_offset as usize,
        )
    }

    pub fn load(file_path: &str, readonly: bool) -> Result<Self> {
        let mmap = Self::load_mapping(file_path, readonly)?;

        let store = DataStore {
            mmap,
            readonly,
            _data: PhantomData,
        };

        Ok(store)
    }

    /// store_length_in_bytes -> (index_offset, index_length, data_offset)
    /// - this should only be used when initializing a new DataStore
    pub fn derive_index_bounds(total_len: usize) -> (usize, usize, usize) {
        let offset = std::mem::size_of::<DataStoreHeader>();
        let ihead = std::mem::size_of::<DataStoreIndex>();
        let phead = std::mem::size_of::<DataStorePager>();
        let base_items = 1 + (total_len >> 10);
        let rup_items = round_up(base_items, 10);
        let sub_bytes = offset + ihead + phead;
        let sub_items = sub_bytes / std::mem::size_of::<u32>();
        let index_length = rup_items - sub_items;
        let data_offset = offset + ihead + index_length * std::mem::size_of::<u32>();

        (offset, index_length, data_offset)
    }

    pub fn init(file_path: &str) -> Result<Self> {
        let readonly = false;
        let mapping = Self::load_mapping(file_path, readonly)?;

        let total_length = mapping.len();
        let (index_offset, index_length, data_offset) = Self::derive_index_bounds(total_length);
        let index_modulo_max = index_length * 99 / 100;
        let index_modulo = sieve::get_le_prime(index_modulo_max as u32);

        let mut map = mapping.try_write()?;

        unsafe {
            DataStoreIndex::init_at_ptr(
                map[index_offset..index_offset].as_mut_ptr(),
                (),
                index_length,
            )
        }
        .fill(0);

        unsafe {
            DataStorePager::init_at_ptr(
                map[data_offset..data_offset].as_mut_ptr(),
                (),
                (total_length - data_offset) / CHUNK_SIZE,
            );
        }

        let mut guard = DataStoreWriteGuard::from(map);
        let header = guard.get_header();

        header.magic = MAGIC;
        header.index_modulo = index_modulo;
        header.free_chunk = 0;
        header.index_offset = index_offset as u64;
        header.data_offset = data_offset as u64;

        let store = Self::load(file_path, false)?;

        store.put_opaque_chunk(&OwnedDataChunk::from_data_ref(
            b"<< DATA SEGMENT BEGINS HERE >>",
            // Chunk #0 cannot be accessed and is therefore reserved for metadata
            // about this DataStore, the size of which shall not exceed 192 bytes
        ))?;

        Ok(store)
    }

    pub fn get_chunk_by_index(&self, index: usize) -> Result<MbufDataChunk> {
        match self.shared().get_pager().get(index) {
            Some(page) => Ok(page.mbuf().into()),
            None => Err(PsDataLakeError::RangeError),
        }
    }

    pub fn calculate_index_bucket(hash: &[u8], index_modulo: u32) -> u32 {
        ps_hash::checksum_u32(hash, hash.len() as u32) % index_modulo
    }

    pub fn get_bucket_index_chunk_by_hash(
        &self,
        hash: &[u8],
    ) -> Result<(u32, u32, Option<MbufDataChunk>)> {
        let shared = self.shared();
        let header = shared.get_header();
        let index = shared.get_index();
        let bucket = Self::calculate_index_bucket(hash, header.index_modulo);

        for bucket in bucket..index.len() as u32 {
            let index = index
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

    pub fn get_bucket_by_hash(&'lt self, hash: &[u8]) -> Result<u32> {
        let (bucket, _, _) = self.get_bucket_index_chunk_by_hash(hash)?;

        Ok(bucket)
    }

    pub fn get_index_by_hash(&'lt self, hash: &[u8]) -> Result<u32> {
        let (_, index, chunk) = self.get_bucket_index_chunk_by_hash(hash)?;

        chunk.ok_or(PsDataLakeError::NotFound)?;

        Ok(index)
    }

    pub fn get_chunk_by_hash(&'lt self, hash: &[u8]) -> Result<MbufDataChunk<'lt>> {
        let (_, _, chunk) = self.get_bucket_index_chunk_by_hash(hash)?;

        chunk.ok_or(PsDataLakeError::NotFound)
    }

    pub fn get_chunk_by_hkey(&'lt self, key: &Hkey) -> Result<DataChunk> {
        match key {
            Hkey::Raw(raw) => Ok(OwnedDataChunk::from_data_ref(raw).into()),
            Hkey::Base64(base64) => {
                Ok(OwnedDataChunk::from_data(ps_base64::decode(base64.as_bytes())).into())
            }
            Hkey::Direct(hash) => Ok(self.get_chunk_by_hash(hash.as_bytes())?.into()),
            Hkey::Encrypted(hash, key) => {
                let chunk = self.get_chunk_by_hash(hash.as_bytes())?;
                let decrypted = chunk.decrypt(key.as_bytes())?;

                Ok(decrypted.into())
            }
            Hkey::ListRef(hash, key) => {
                let hkey = Hkey::Encrypted(hash.clone(), key.clone());
                let long = Self::get_chunk_by_hkey(self, &hkey)?;
                let hkey = Hkey::parse(long.data_ref());

                self.get_chunk_by_hkey(&hkey)
            }
            Hkey::List(list) => {
                // Parallel fetching of chunks
                let chunks: Vec<Result<DataChunk>> = list
                    .par_iter()
                    .map(|hkey| self.get_chunk_by_hkey(hkey))
                    .collect();

                // Collect and concatenate data from chunks
                let buffer: Result<Vec<u8>> =
                    chunks.into_iter().try_fold(vec![], |mut buffer, chunk| {
                        buffer.extend_from_slice(chunk?.data_ref());
                        Ok(buffer)
                    });

                // Convert the concatenated data into an OwnedDataChunk
                let chunk = OwnedDataChunk::from_data(buffer?);

                Ok(chunk.into())
            }
            _ => key.resolve(&|hash| Ok(self.get_chunk_by_hash(hash.as_bytes())?.into())),
        }
    }

    /// Stores opaque data and returns a tuple containing the bucket, index, and DataChunk.
    ///
    /// # Arguments
    ///
    /// * `opaque_chunk` - A DataChunk representing the opaque data to be stored.
    ///
    /// # Returns
    ///
    /// A `Result` containing:
    /// - `Ok((bucket, index, datachunk))` on success:
    ///   - `bucket` (u32): The hashmap index of the chunk.
    ///   - `index` (u32): The chunk's index withing the storage file.
    ///   - `datachunk` (MbufDataChunk): The chunk of data that was stored.
    /// - `Err(PsDataLakeError)` on failure:
    ///   - An error of type `PsDataLakeError` indicating the reason for failure.
    pub fn put_opaque_chunk<C: DataChunkTrait>(
        &self,
        opaque_chunk: &C,
    ) -> Result<(u32, u32, MbufDataChunk)> {
        let existing = self.get_bucket_index_chunk_by_hash(opaque_chunk.hash_ref())?;
        let (bucket, index, existing) = existing;

        if let Some(chunk) = existing {
            return Ok((bucket, index, chunk));
        }

        if self.readonly {
            Err(PsDataLakeError::DataStoreNotRw)?
        }

        let mut atomic = self.atomic()?;

        let next_free_chunk = atomic.get_header().free_chunk as usize;

        let required_chunks = DataStorePage::bytes_to_pages(opaque_chunk.data_ref().len());

        if (atomic.get_pager().len() - next_free_chunk) < required_chunks {
            Err(PsDataLakeError::DataStoreOutOfSpace)?
        }

        let pointer = atomic
            .get_pager()
            .get(next_free_chunk)
            .ok_or(PsDataLakeError::RangeError)?
            .mbuf() as *const _ as *mut u8;

        unsafe {
            DataStorePageMbuf::write_to_ptr(pointer, *opaque_chunk.hash(), opaque_chunk.data_ref())
        };

        atomic.get_header().free_chunk += required_chunks as u32;

        atomic.get_index()[bucket as usize] = next_free_chunk as u32;

        drop(atomic);

        Ok((
            bucket,
            next_free_chunk as u32,
            self.get_chunk_by_index(next_free_chunk)?,
        ))
    }

    pub fn put_encrypted_chunk<C: DataChunkTrait>(&'lt self, chunk: &C) -> Result<Hkey> {
        let length = chunk.data_ref().len();

        if !(DATA_CHUNK_MIN_SIZE..=DATA_CHUNK_MAX_ENCRYPTED_SIZE).contains(&length) {
            return self.put_chunk(chunk);
        }

        let encrypted = chunk.encrypt()?;

        if encrypted.data_ref().len() > length {
            Ok(self.put_opaque_chunk(chunk)?.2.hash().into())
        } else {
            let chunk = self.put_opaque_chunk(&encrypted.chunk)?.2;

            if chunk.hash_ref() != encrypted.chunk.hash_ref() {
                Err(PsDataLakeError::StorageFailure)?
            }

            Ok((encrypted.hash(), encrypted.key).into())
        }
    }

    pub fn put_large_chunk<C: DataChunkTrait>(&'lt self, chunk: &C) -> Result<Hkey> {
        self.put_large_blob(chunk.data_ref())
    }

    pub fn put_chunk<C: DataChunkTrait>(&'lt self, chunk: &C) -> Result<Hkey> {
        if chunk.data_ref().len() < DATA_CHUNK_MIN_SIZE {
            return Ok(Hkey::from_raw(chunk.data_ref()));
        }

        if chunk.data_ref().len() > DATA_CHUNK_MAX_RAW_SIZE {
            return self.put_large_chunk(chunk);
        }

        let encrypted = chunk.encrypt()?;

        let stored_chunk = self.put_opaque_chunk(&encrypted.chunk)?.2;

        if stored_chunk.hash_ref() != encrypted.hash_ref() {
            Err(PsDataLakeError::StorageFailure)?
        }

        Ok(Hkey::Encrypted(encrypted.chunk.hash(), encrypted.key))
    }

    pub fn put_large_blob(&'lt self, blob: &[u8]) -> Result<Hkey> {
        // sanity check
        if blob.len() < DATA_CHUNK_MAX_RAW_SIZE {
            return self.put_blob(blob);
        }

        let store = |blob: &[u8]| self.put_blob(blob);

        LongHkeyExpanded::from_blob::<PsDataLakeError, _, _>(&store, blob)?.shrink(&store)
    }

    pub fn put_blob(&'lt self, blob: &[u8]) -> Result<Hkey> {
        if blob.len() < DATA_CHUNK_MIN_SIZE {
            Ok(Hkey::from_raw(blob))
        } else if blob.len() > DATA_CHUNK_MAX_RAW_SIZE {
            self.put_large_blob(blob)
        } else {
            self.put_chunk(&BorrowedDataChunk::from_data(blob))
        }
    }
}
