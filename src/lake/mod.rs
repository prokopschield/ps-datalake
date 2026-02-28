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
use util::verify_magic;

#[derive(Clone, Debug, Default)]
pub struct DataLakeStores<'lt> {
    pub readable: Vec<DataStore<'lt>>,
    pub writable: Vec<DataStore<'lt>>,
}

#[derive(Clone, Debug, Default)]
pub struct DataLake<'lt> {
    pub config: DataLakeConfig,
    pub stores: DataLakeStores<'lt>,
}

impl<'lt> DataLake<'lt> {
    pub fn init(config: DataLakeConfig) -> Result<Self> {
        let mut stores = DataLakeStores::default();

        for entry in &config.stores {
            if entry.readonly {
                let store = DataStore::load(&entry.filename, true)?;

                stores.readable.push(store);

                continue;
            }

            let store = if verify_magic(&entry.filename)? {
                DataStore::load(&entry.filename, false)
            } else {
                DataStore::init(&entry.filename)
            }?;

            stores.readable.push(store.clone());
            stores.writable.push(store);
        }

        let lake = Self { config, stores };

        Ok(lake)
    }

    pub fn get_encrypted_chunk(&'lt self, hash: &Hash) -> Result<MbufDataChunk<'lt>> {
        let mut error = DataLakeError::NotFound;

        for store in &self.stores.readable {
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

    pub fn put_encrypted_chunk<C: DataChunk>(&'lt self, chunk: &C) -> Result<Hkey> {
        use DataLakeError::{DataStoreNotRw, DataStoreOutOfSpace};

        for store in &self.stores.writable {
            match store.put_encrypted_chunk(chunk) {
                Ok(chunk) => return Ok(chunk),
                Err(err) => match err {
                    DataStoreOutOfSpace | DataStoreNotRw => (),
                    err => Err(err)?,
                },
            }
        }

        Err(DataLakeError::DataLakeOutOfStores)
    }

    pub fn put_chunk<C: DataChunk>(&'lt self, chunk: &C) -> Result<Hkey> {
        for store in &self.stores.writable {
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

    pub fn put_blob(&'lt self, blob: &[u8]) -> Result<Hkey> {
        for store in &self.stores.writable {
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

    fn put_encrypted<C: DataChunk>(&self, chunk: C) -> std::result::Result<(), Self::Error> {
        self.put_encrypted_chunk(&chunk)?;
        Ok(())
    }
}
