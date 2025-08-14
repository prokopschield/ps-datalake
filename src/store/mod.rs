mod atomic;
mod shared;

use std::marker::PhantomData;
use std::path::Path;

use crate::error::DataStoreCorrupted;
use crate::error::PsDataLakeError;
use crate::error::Result;
use crate::helpers::sieve;
use atomic::DataStoreWriteGuard;
use ps_base64::base64;
use ps_datachunk::utils::round_up;
use ps_datachunk::BorrowedDataChunk;
use ps_datachunk::DataChunk;
use ps_datachunk::MbufDataChunk;
use ps_datachunk::OwnedDataChunk;
use ps_hash::Hash;
use ps_hkey::Hkey;
use ps_hkey::LongHkeyExpanded;
use ps_hkey::Resolved;
use ps_hkey::Store;
use ps_hkey::MAX_DECRYPTED_SIZE;
use ps_hkey::MAX_ENCRYPTED_SIZE;
use ps_hkey::MAX_SIZE_RAW;
use ps_mbuf::Mbuf;
use ps_mmap::MemoryMap;
use ps_str::Utf8Encoder;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use shared::DataStoreReadGuard;

pub const MAGIC: [u8; 16] = *b"DataLake\0\0\0\0\0\0\0\0";
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
    #[must_use]
    pub const fn mbuf(&'lt self) -> &'lt DataStorePageMbuf<'lt> {
        &self.mbuf
    }
    #[must_use]
    pub const fn bytes_to_pages(bytes: usize) -> usize {
        (bytes + std::mem::size_of::<DataStorePageMbuf>()).div_ceil(CHUNK_SIZE)
    }
}

pub type DataStoreIndex<'lt> = Mbuf<'lt, (), u32>;
pub type DataStorePager<'lt> = Mbuf<'lt, (), DataStorePage<'lt>>;

#[derive(Clone, Debug)]
pub struct DataStore<'lt> {
    mmap: MemoryMap,
    readonly: bool,
    _data: PhantomData<&'lt [u8]>,
}

impl<'lt> DataStore<'lt> {
    pub fn load_mapping<P>(file_path: P, readonly: bool) -> Result<MemoryMap>
    where
        P: AsRef<Path>,
    {
        Ok(MemoryMap::map(file_path, readonly)?)
    }

    fn shared(&self) -> DataStoreReadGuard<'lt> {
        self.into()
    }

    fn atomic(&self) -> Result<DataStoreWriteGuard> {
        Ok(self.try_into()?)
    }

    #[must_use]
    pub unsafe fn get_header(mapping: &MemoryMap) -> &'lt mut DataStoreHeader {
        &mut *(mapping.as_ptr() as *mut DataStoreHeader)
    }

    #[must_use]
    pub unsafe fn get_index(mapping: &MemoryMap) -> &'lt mut DataStoreIndex<'lt> {
        DataStoreIndex::at_offset_mut(
            mapping.as_ptr().cast_mut(),
            Self::get_header(mapping).index_offset as usize,
        )
    }

    #[must_use]
    pub unsafe fn get_pager(mapping: &MemoryMap) -> &'lt mut DataStorePager<'lt> {
        DataStorePager::at_offset_mut(
            mapping.as_ptr().cast_mut(),
            Self::get_header(mapping).data_offset as usize,
        )
    }

    pub fn load<P>(file_path: P, readonly: bool) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        use DataStoreCorrupted::{
            DataEndsOutOfBounds, DataOffsetOutOfBounds, IndexDataOverlap, IndexEndsOutOfBounds,
            IndexModuloTooSmall, IndexOffsetOutOfBounds, InvalidDataStoreSize, InvalidMagic,
        };

        let mmap = Self::load_mapping(file_path, readonly)?;
        let size = mmap.len();

        if size < std::mem::size_of::<DataStoreHeader>() {
            Err(InvalidDataStoreSize(size))?;
        }

        let store = DataStore {
            mmap,
            readonly,
            _data: PhantomData,
        };

        let shared = store.shared();
        let header = shared.get_header();

        if header.magic != MAGIC {
            Err(InvalidMagic(header.magic.to_utf8_string()))?;
        }

        if header.index_offset > size as u64 {
            Err(IndexOffsetOutOfBounds(header.index_offset, size))?;
        }

        if header.data_offset > size as u64 {
            Err(DataOffsetOutOfBounds(header.data_offset, size))?;
        }

        let index = shared.get_index();
        let pager = shared.get_pager();

        let base_ptr = store.mmap.as_ptr();

        let index_end_offset = index.as_ptr_range().end as usize - base_ptr as usize;
        if index_end_offset > size {
            Err(IndexEndsOutOfBounds(index_end_offset, size))?;
        }

        let data_end_offset = pager.as_ptr_range().end as usize - base_ptr as usize;
        if data_end_offset > size {
            Err(DataEndsOutOfBounds(data_end_offset, size))?;
        }

        let data_ptr = std::ptr::addr_of!(*pager.get_metadata());
        let data_start_offset = data_ptr as usize;
        if index_end_offset > data_start_offset {
            Err(IndexDataOverlap(index_end_offset, data_start_offset))?;
        }

        if header.index_modulo > (index.len() as u32) {
            Err(IndexModuloTooSmall(header.index_modulo, index.len() as u32))?;
        }

        Ok(store)
    }

    /// `store_length_in_bytes` -> (`index_offset`, `index_length`, `data_offset`)
    /// - this should only be used when initializing a new [`DataStore`]
    #[must_use]
    pub const fn derive_index_bounds(total_len: usize) -> (usize, usize, usize) {
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

    pub fn init<P>(file_path: &P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let readonly = false;
        let mapping = Self::load_mapping(file_path, readonly)?;

        let total_length = mapping.len();
        let (index_offset, index_length, data_offset) = Self::derive_index_bounds(total_length);
        let index_modulo_max = index_length * 99 / 100;
        let index_modulo = sieve::get_le_prime(index_modulo_max as u32);

        if (data_offset + std::mem::size_of::<DataStorePager>()) > mapping.len() {
            Err(PsDataLakeError::InitFailedNotEnoughSpace(mapping.len()))?;
        }

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

        drop(guard);

        let store = Self::load(file_path, false)?;

        store.put_opaque_chunk(&BorrowedDataChunk::from_data(
            b"<< DATA SEGMENT BEGINS HERE >>",
            // Chunk #0 cannot be accessed and is therefore reserved for metadata
            // about this DataStore, the size of which shall not exceed 192 bytes
        )?)?;

        Ok(store)
    }

    pub fn get_chunk_by_index(&self, index: usize) -> Result<MbufDataChunk> {
        match self.shared().get_pager().get(index) {
            Some(page) => Ok(page.mbuf().into()),
            None => Err(PsDataLakeError::RangeError),
        }
    }

    #[must_use]
    pub fn calculate_index_bucket(hash: &Hash, index_modulo: u32) -> u32 {
        (u32::from_be_bytes(base64::sized_decode::<4>(&hash.as_bytes()[..6]))) % index_modulo
    }

    pub fn get_bucket_index_chunk_by_hash(
        &self,
        hash: &Hash,
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

    pub fn get_bucket_by_hash(&'lt self, hash: &Hash) -> Result<u32> {
        let (bucket, _, _) = self.get_bucket_index_chunk_by_hash(hash)?;

        Ok(bucket)
    }

    pub fn get_index_by_hash(&'lt self, hash: &Hash) -> Result<u32> {
        let (_, index, chunk) = self.get_bucket_index_chunk_by_hash(hash)?;

        chunk.ok_or(PsDataLakeError::NotFound)?;

        Ok(index)
    }

    pub fn get_chunk_by_hash(&'lt self, hash: &Hash) -> Result<MbufDataChunk<'lt>> {
        let (_, _, chunk) = self.get_bucket_index_chunk_by_hash(hash)?;

        chunk.ok_or(PsDataLakeError::NotFound)
    }

    pub fn get_chunk_by_hkey(&'lt self, key: &Hkey) -> Result<Resolved<MbufDataChunk<'lt>>> {
        match key {
            Hkey::Raw(raw) => Ok(Resolved::Data(raw.clone())),
            Hkey::Base64(base64) => {
                Ok(OwnedDataChunk::from_data(ps_base64::decode(base64.as_bytes()))?.into())
            }
            Hkey::Direct(hash) => Ok(Resolved::Custom(self.get_chunk_by_hash(hash)?)),
            Hkey::Encrypted(hash, key) => {
                let chunk = self.get_chunk_by_hash(hash)?;
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
                let chunks: Vec<Result<Resolved<MbufDataChunk>>> = list
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
                let chunk = OwnedDataChunk::from_data(buffer?)?;

                Ok(chunk.into())
            }
            _ => key.resolve(self),
        }
    }

    /// Stores opaque data and returns a tuple containing the bucket, index, and [`DataChunk`].
    ///
    /// # Arguments
    ///
    /// * `opaque_chunk` - A [`DataChunk`] representing the opaque data to be stored.
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
    pub fn put_opaque_chunk<C: DataChunk>(
        &self,
        opaque_chunk: &C,
    ) -> Result<(u32, u32, MbufDataChunk)> {
        let existing = self.get_bucket_index_chunk_by_hash(opaque_chunk.hash_ref())?;
        let (bucket, index, existing) = existing;

        if let Some(chunk) = existing {
            return Ok((bucket, index, chunk));
        }

        if self.readonly {
            Err(PsDataLakeError::DataStoreNotRw)?;
        }

        let mut atomic = self.atomic()?;

        let next_free_chunk = atomic.get_header().free_chunk as usize;

        let required_chunks = DataStorePage::bytes_to_pages(opaque_chunk.data_ref().len());
        let available_chunks = atomic
            .get_pager()
            .len()
            .checked_sub(next_free_chunk)
            .ok_or(PsDataLakeError::DataStoreOutOfSpace)?;

        if available_chunks < required_chunks {
            Err(PsDataLakeError::DataStoreOutOfSpace)?;
        }

        let pointer = std::ptr::from_ref(
            atomic
                .get_pager()
                .get(next_free_chunk)
                .ok_or(PsDataLakeError::RangeError)?
                .mbuf(),
        ) as *mut u8;

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

    pub fn put_encrypted_chunk<C: DataChunk>(&'lt self, chunk: &C) -> Result<Hkey> {
        let length = chunk.data_ref().len();

        if length <= MAX_SIZE_RAW || length > MAX_ENCRYPTED_SIZE {
            return self.put_chunk(chunk);
        }

        let encrypted = chunk.encrypt()?;

        if encrypted.data_ref().len() > length {
            Ok(self.put_opaque_chunk(chunk)?.2.hash().into())
        } else {
            let chunk = self.put_opaque_chunk(&encrypted)?.2;

            if chunk.hash_ref() != encrypted.hash_ref() {
                Err(PsDataLakeError::StorageFailure)?;
            }

            Ok((encrypted.hash(), encrypted.key()).into())
        }
    }

    pub fn put_large_chunk<C: DataChunk>(&'lt self, chunk: &C) -> Result<Hkey> {
        self.put_large_blob(chunk.data_ref())
    }

    pub fn put_chunk<C: DataChunk>(&'lt self, chunk: &C) -> Result<Hkey> {
        if chunk.data_ref().len() <= MAX_SIZE_RAW {
            return Ok(Hkey::from_raw(chunk.data_ref()));
        }

        if chunk.data_ref().len() > MAX_DECRYPTED_SIZE {
            return self.put_large_chunk(chunk);
        }

        let encrypted = chunk.encrypt()?;

        let stored_chunk = self.put_opaque_chunk(&encrypted)?.2;

        if stored_chunk.hash_ref() != encrypted.hash_ref() {
            Err(PsDataLakeError::StorageFailure)?;
        }

        Ok(Hkey::Encrypted(encrypted.hash(), encrypted.key()))
    }

    pub fn put_large_blob(&'lt self, blob: &[u8]) -> Result<Hkey> {
        // sanity check
        if blob.len() <= MAX_DECRYPTED_SIZE {
            return self.put_blob(blob);
        }

        LongHkeyExpanded::from_blob(self, blob)?.shrink(self)
    }

    pub fn put_blob(&'lt self, blob: &[u8]) -> Result<Hkey> {
        if blob.len() <= MAX_SIZE_RAW {
            Ok(Hkey::from_raw(blob))
        } else if blob.len() > MAX_DECRYPTED_SIZE {
            self.put_large_blob(blob)
        } else {
            self.put_chunk(&BorrowedDataChunk::from_data(blob)?)
        }
    }
}

impl<'lt> Store for DataStore<'lt> {
    type Chunk<'c>
        = MbufDataChunk<'c>
    where
        'lt: 'c;
    type Error = PsDataLakeError;

    fn get<'a>(&'a self, hash: &Hash) -> std::result::Result<Self::Chunk<'a>, Self::Error> {
        self.get_chunk_by_hash(hash)
    }

    fn put(&self, data: &[u8]) -> std::result::Result<Hkey, Self::Error> {
        self.put_blob(data)
    }

    fn put_encrypted<C: DataChunk>(&self, chunk: C) -> std::result::Result<(), Self::Error> {
        self.put_encrypted_chunk(&chunk)?;
        Ok(())
    }
}
