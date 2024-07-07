pub mod config;
use crate::error::PsDataLakeError;
use crate::error::Result;
use crate::store::DataStore;
use ps_datachunk::Compressor;
use ps_datachunk::DataChunk;
use ps_datachunk::DataChunkTrait;
use ps_datachunk::MbufDataChunk;
use ps_hkey::Hkey;

#[derive(Clone, Default)]
pub struct DataLakeStores<'lt> {
    pub readable: Vec<DataStore<'lt>>,
    pub writable: Vec<DataStore<'lt>>,
}

pub struct DataLake<'lt> {
    pub stores: DataLakeStores<'lt>,
}

impl<'lt> DataLake<'lt> {
    pub fn get_encrypted_chunk(&'lt self, hash: &[u8]) -> Result<MbufDataChunk> {
        let mut error = PsDataLakeError::NotFound;

        for store in &self.stores.readable {
            match store.get_chunk_by_hash(hash) {
                Ok(chunk) => return Ok(chunk),
                Err(err) => match err {
                    PsDataLakeError::NotFound => (),
                    _ => error = err,
                },
            }
        }

        Err(error)
    }

    pub fn get_chunk_by_hkey(&'lt self, hkey: &Hkey) -> Result<DataChunk> {
        hkey.resolve(&|hash| match self.get_encrypted_chunk(hash.as_bytes()) {
            Ok(chunk) => Ok(chunk.into()),
            Err(err) => Err(err),
        })
    }

    pub fn put_encrypted_chunk<C: DataChunkTrait>(
        &'lt self,
        chunk: &C,
        compressor: &Compressor,
    ) -> Result<Hkey> {
        for store in &self.stores.writable {
            match store.put_encrypted_chunk(chunk, compressor) {
                Ok(chunk) => return Ok(chunk),
                Err(err) => match err {
                    PsDataLakeError::DataStoreOutOfSpace => (),
                    PsDataLakeError::DataStoreNotRw => (),
                    err => Err(err)?,
                },
            }
        }

        Err(PsDataLakeError::DataLakeOutOfStores)
    }

    pub fn put_chunk<C: DataChunkTrait>(
        &'lt self,
        chunk: &C,
        compressor: &Compressor,
    ) -> Result<Hkey> {
        for store in &self.stores.writable {
            match store.put_chunk(chunk, compressor) {
                Ok(chunk) => return Ok(chunk),
                Err(err) => match err {
                    PsDataLakeError::DataStoreOutOfSpace => (),
                    PsDataLakeError::DataStoreNotRw => (),
                    _ => Err(err)?,
                },
            }
        }

        Err(PsDataLakeError::DataLakeOutOfStores)
    }

    pub fn put_blob(&'lt self, blob: &[u8], compressor: &Compressor) -> Result<Hkey> {
        for store in &self.stores.writable {
            match store.put_blob(blob, compressor) {
                Ok(chunk) => return Ok(chunk),
                Err(err) => match err {
                    PsDataLakeError::DataStoreOutOfSpace => (),
                    PsDataLakeError::DataStoreNotRw => (),
                    _ => Err(err)?,
                },
            }
        }

        Err(PsDataLakeError::DataLakeOutOfStores)
    }
}
