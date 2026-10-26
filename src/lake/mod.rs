pub mod config;
pub mod util;
use crate::error::DataLakeError;
use crate::error::Result;
use crate::store::DataStore;
use config::DataLakeConfig;
use ps_datachunk::DataChunk;
use ps_datachunk::MbufDataChunk;
use ps_hash::Hash;
use ps_hkey::Hkey;
use ps_hkey::Store;
use std::sync::Arc;
use util::verify_magic;

#[derive(Clone, Debug, Default)]
pub struct DataLakeStores<'lt> {
    pub readable: Arc<[DataStore<'lt>]>,
    pub writable: Arc<[DataStore<'lt>]>,
}

#[derive(Clone, Debug, Default)]
pub struct DataLake<'lt> {
    pub stores: DataLakeStores<'lt>,
}

impl<'lt> DataLake<'lt> {
    pub fn init(config: &DataLakeConfig) -> Result<Self> {
        let mut readable = vec![];
        let mut writable = vec![];

        for entry in &config.stores {
            if entry.readonly {
                let loaded_ro_stores = DataStore::load_all(&entry.filename, true)?;

                readable.extend(loaded_ro_stores);

                continue;
            }

            if verify_magic(&entry.filename)? {
                let loaded_stores = DataStore::load_all(&entry.filename, false)?;

                readable.extend(loaded_stores.iter().cloned());
                writable.extend(loaded_stores);
            } else {
                let stores_init = DataStore::init(&entry.filename)?;

                readable.extend(stores_init.iter().cloned());
                writable.extend(stores_init);
            }
        }

        let stores = DataLakeStores {
            readable: readable.into(),
            writable: writable.into(),
        };

        let lake = Self { stores };

        Ok(lake)
    }

    pub fn get_encrypted_chunk(&'lt self, hash: &Hash) -> Result<MbufDataChunk<'lt>> {
        let mut error = DataLakeError::NotFound;

        for store in &*self.stores.readable {
            match store.get_chunk_by_hash(hash) {
                Ok(chunk) => return Ok(chunk),
                Err(err) => match err {
                    DataLakeError::NotFound => (),
                    _ => error = err,
                },
            }
        }

        Err(error)
    }

    pub fn put_chunk<C: DataChunk>(&'lt self, chunk: &C) -> Result<Hkey> {
        for store in &*self.stores.writable {
            match store.put_chunk(chunk) {
                Ok(chunk) => return Ok(chunk),
                Err(err) => match err {
                    DataLakeError::DataStoreOutOfSpace | DataLakeError::DataStoreNotRw => (),
                    _ => Err(err)?,
                },
            }
        }

        Err(DataLakeError::DataLakeOutOfStores)
    }

    /// Stores `chunk` verbatim, under its own hash, in the first writable store with room for it.
    pub fn put_opaque_chunk<C: DataChunk>(&self, chunk: &C) -> Result<()> {
        for store in &*self.stores.writable {
            match store.put_opaque_chunk(chunk) {
                Ok(_) => return Ok(()),
                Err(err) => match err {
                    DataLakeError::DataStoreOutOfSpace | DataLakeError::DataStoreNotRw => (),
                    _ => Err(err)?,
                },
            }
        }

        Err(DataLakeError::DataLakeOutOfStores)
    }

    pub fn put_blob(&'lt self, blob: &[u8]) -> Result<Hkey> {
        for store in &*self.stores.writable {
            match store.put_blob(blob) {
                Ok(chunk) => return Ok(chunk),
                Err(err) => match err {
                    DataLakeError::DataStoreOutOfSpace | DataLakeError::DataStoreNotRw => (),
                    _ => Err(err)?,
                },
            }
        }

        Err(DataLakeError::DataLakeOutOfStores)
    }
}

impl<'lt> Store for DataLake<'lt> {
    type Chunk<'c>
        = MbufDataChunk<'c>
    where
        'lt: 'c;
    type Error = DataLakeError;

    fn get<'a>(&'a self, hash: &Hash) -> std::result::Result<Self::Chunk<'a>, Self::Error> {
        self.get_encrypted_chunk(hash)
    }

    fn put(&self, data: &[u8]) -> std::result::Result<Hkey, Self::Error> {
        self.put_blob(data)
    }

    fn put_verbatim<C: DataChunk>(&self, chunk: C) -> std::result::Result<(), Self::Error> {
        self.put_opaque_chunk(&chunk)
    }
}
