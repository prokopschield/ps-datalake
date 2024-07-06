pub mod hkey;
use crate::error::PsDataLakeError;
use crate::error::Result;
use crate::helpers::sieve;
use hkey::Hkey;
use parking_lot::Mutex;
use ps_datachunk::aligned::rup;
use ps_datachunk::BorrowedDataChunk;
use ps_datachunk::Compressor;
use ps_datachunk::DataChunk;
use ps_datachunk::DataChunkTrait;
use ps_datachunk::MbufDataChunk;
use ps_datachunk::OwnedDataChunk;
use ps_hash::Hash;
use ps_mbuf::Mbuf;
use ps_mmap::MemoryMapping;
use ps_mmap::MmapOptions;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use rayon::slice::ParallelSlice;
use std::sync::Arc;

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

#[derive(Clone)]
pub struct DataStore<'lt> {
    shared: DataStoreShared<'lt>,
    atomic: Arc<Mutex<DataStoreAtomic<'lt>>>,
    readonly: bool,
}

impl<'lt> DataStore<'lt> {
    pub fn load_mapping(file_path: &'lt str, readonly: bool) -> Result<MemoryMapping<'lt>> {
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

    pub fn load(file_path: &'lt str, readonly: bool) -> Result<Self> {
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

    /// store_length_in_bytes -> (index_offset, index_length, data_offset)
    /// - this should only be used when initializing a new DataStore
    pub fn derive_index_bounds(total_len: usize) -> (usize, usize, usize) {
        let offset = std::mem::size_of::<DataStoreHeader>();
        let ihead = std::mem::size_of::<DataStoreIndex>();
        let phead = std::mem::size_of::<DataStorePager>();
        let base_items = 1 + (total_len >> 10);
        let rup_items = rup(base_items, 10);
        let sub_bytes = offset + ihead + phead;
        let sub_items = sub_bytes / std::mem::size_of::<u32>();
        let index_length = rup_items - sub_items;
        let data_offset = offset + ihead + index_length * std::mem::size_of::<u32>();

        (offset, index_length, data_offset)
    }

    pub fn init(file_path: &'lt str) -> Result<Self> {
        let readonly = false;
        let mapping = Self::load_mapping(file_path, readonly)?;
        let header = unsafe { Self::get_header(&mapping) };

        let total_length = mapping.len();
        let (index_offset, index_length, data_offset) = Self::derive_index_bounds(total_length);
        let index_modulo = sieve::get_le_prime(index_length as u32);

        let arc = mapping.rw()?;
        let mut map = arc.lock()?;

        unsafe {
            DataStoreIndex::init_at_ptr(
                (&mut map[index_offset..index_offset]).as_mut_ptr(),
                (),
                index_length,
            )
        }
        .fill(0);

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

        let store = Self::load(file_path, false)?;

        store.put_opaque_chunk(&OwnedDataChunk::from_data_ref(
            b"<< DATA SEGMENT BEGINS HERE >>",
            // Chunk #0 cannot be accessed and is therefore reserved for metadata
            // about this DataStore, the size of which shall not exceed 192 bytes
        ))?;

        Ok(store)
    }

    pub fn get_chunk_by_index(&self, index: usize) -> Result<MbufDataChunk> {
        match self.shared.pager.get(index) {
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

        Ok(chunk.ok_or(PsDataLakeError::NotFound)?)
    }

    pub fn get_chunk_by_hkey(&'lt self, key: &Hkey, compressor: &Compressor) -> Result<DataChunk> {
        match key {
            Hkey::Raw(raw) => Ok(OwnedDataChunk::from_data_ref(raw).into()),
            Hkey::Base64(base64) => {
                Ok(OwnedDataChunk::from_data(ps_base64::decode(base64.as_bytes())).into())
            }
            Hkey::Direct(hash) => Ok(self.get_chunk_by_hash(hash.as_bytes())?.into()),
            Hkey::Encrypted(hash, key) => {
                let chunk = self.get_chunk_by_hash(hash.as_bytes())?;
                let decrypted = chunk.decrypt(key.as_bytes(), compressor)?;

                Ok(decrypted.into())
            }
            Hkey::ListRef(hash, key) => {
                let hkey = Hkey::Encrypted(hash.clone(), key.clone());
                let long = Self::get_chunk_by_hkey(self, &hkey, compressor)?;
                let hkey = Hkey::parse(long.data_ref());

                self.get_chunk_by_hkey(&hkey, compressor)
            }
            Hkey::List(list) => {
                // Parallel fetching of chunks
                let chunks: Vec<Result<DataChunk>> = list
                    .par_iter()
                    .map(|hkey| self.get_chunk_by_hkey(hkey, &Compressor::new()))
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

        let mut atomic = self.atomic.lock();

        let next_free_chunk = atomic.header.free_chunk as usize;

        let required_chunks = DataStorePage::bytes_to_pages(opaque_chunk.data_ref().len());

        if (atomic.pager.len() - next_free_chunk) < required_chunks {
            Err(PsDataLakeError::DataStoreOutOfSpace)?
        }

        let pointer = atomic
            .pager
            .get(next_free_chunk)
            .ok_or(PsDataLakeError::RangeError)?
            .mbuf() as *const _ as *mut u8;

        unsafe {
            DataStorePageMbuf::write_to_ptr(pointer, *opaque_chunk.hash(), opaque_chunk.data_ref())
        };

        atomic.header.free_chunk += required_chunks as u32;

        atomic.index[bucket as usize] = next_free_chunk as u32;

        drop(atomic);

        Ok((
            bucket,
            next_free_chunk as u32,
            self.get_chunk_by_index(next_free_chunk)?.into(),
        ))
    }

    pub fn put_encrypted_chunk<C: DataChunkTrait>(
        &'lt self,
        chunk: &C,
        compressor: &Compressor,
    ) -> Result<Hkey> {
        let length = chunk.data_ref().len();

        if length < DATA_CHUNK_MIN_SIZE || length > DATA_CHUNK_MAX_ENCRYPTED_SIZE {
            return self.put_chunk(chunk, compressor);
        }

        let encrypted = chunk.encrypt(compressor)?;

        if encrypted.data_ref().len() > length {
            Ok(self.put_opaque_chunk(chunk)?.2.hash().into())
        } else {
            let chunk = self.put_opaque_chunk(&encrypted.chunk)?.2;

            if chunk.hash_ref() != encrypted.chunk.hash_ref() {
                Err(PsDataLakeError::StorageFailure)?
            }

            Ok((encrypted.hash().into(), encrypted.key).into())
        }
    }

    pub fn put_large_chunk<C: DataChunkTrait>(
        &'lt self,
        chunk: &C,
        compressor: &Compressor,
    ) -> Result<Hkey> {
        self.put_large_blob(chunk.data_ref(), compressor)
    }

    pub fn put_chunk<C: DataChunkTrait>(
        &'lt self,
        chunk: &C,
        compressor: &Compressor,
    ) -> Result<Hkey> {
        if chunk.data_ref().len() < DATA_CHUNK_MIN_SIZE {
            return Ok(Hkey::from_raw(chunk.data_ref()));
        }

        if chunk.data_ref().len() > DATA_CHUNK_MAX_RAW_SIZE {
            return self.put_large_chunk(chunk, compressor);
        }

        let encrypted = chunk.encrypt(compressor)?;

        let stored_chunk = self.put_opaque_chunk(&encrypted.chunk)?.2;

        if stored_chunk.hash_ref() != encrypted.hash_ref() {
            Err(PsDataLakeError::StorageFailure)?
        }

        Ok(Hkey::Encrypted(encrypted.chunk.hash(), encrypted.key))
    }

    pub fn put_large_blob(&'lt self, blob: &[u8], compressor: &Compressor) -> Result<Hkey> {
        // sanity check
        if blob.len() < DATA_CHUNK_MAX_RAW_SIZE {
            return self.put_blob(blob, compressor);
        }

        // get parallel iterator
        let chunks = blob.par_chunks(DATA_CHUNK_MAX_RAW_SIZE);

        // store each chunk
        let chunk_keys: Vec<Result<Hkey>> = chunks
            .map(|blob| self.put_blob(blob, &Compressor::new()))
            .collect();

        // transform Vec<Result<Hkey>> into Result<Vec<Hkey>>
        let chunks: Result<Vec<Hkey>> = chunk_keys.into_iter().collect();

        // generate [c1,c2,..,cN]
        let list = Hkey::format_list(&chunks?);

        // store list
        let hkey = self.put_blob(list.as_bytes(), compressor)?;

        // transform Hkey::Encrypted into Hkey::ListRef
        let hkey = match hkey {
            Hkey::Encrypted(hash, key) => Hkey::ListRef(hash, key),
            _ => Err(PsDataLakeError::StorageFailure)?, // this should never happen
        };

        Ok(hkey)
    }

    pub fn put_blob(&'lt self, blob: &[u8], compressor: &Compressor) -> Result<Hkey> {
        if blob.len() < DATA_CHUNK_MIN_SIZE {
            Ok(Hkey::from_raw(blob))
        } else if blob.len() > DATA_CHUNK_MAX_RAW_SIZE {
            self.put_large_blob(blob, compressor)
        } else {
            self.put_chunk(&BorrowedDataChunk::from_data(blob), compressor)
        }
    }
}
