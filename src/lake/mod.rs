pub mod config;
use crate::error::PsDataLakeError;
use crate::error::Result;
use crate::store::hkey::Hkey;
use crate::store::DataStore;
use ps_datachunk::Compressor;
use ps_datachunk::DataChunk;
use ps_datachunk::MbufDataChunk;
use ps_datachunk::OwnedDataChunk;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;

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

    pub fn get_chunk_by_hkey(&'lt self, hkey: &Hkey, compressor: &Compressor) -> Result<DataChunk> {
        let chunk: DataChunk = match hkey {
            Hkey::Raw(raw) => OwnedDataChunk::from_data_ref(raw).into(),
            Hkey::Base64(base64) => {
                OwnedDataChunk::from_data(ps_base64::decode(base64.as_bytes())).into()
            }
            Hkey::Direct(hash) => {
                for store in &self.stores.readable {
                    match store.get_chunk_by_hash(hash.as_bytes()) {
                        Ok(chunk) => return Ok(chunk.into()),
                        _ => (),
                    }
                }
                Err(PsDataLakeError::NotFound)?
            }
            Hkey::Encrypted(_, _) => {
                for store in &self.stores.readable {
                    match store.get_chunk_by_hkey(hkey, compressor) {
                        Ok(chunk) => return Ok(chunk),
                        _ => (),
                    }
                }
                Err(PsDataLakeError::NotFound)?
            }
            Hkey::ListRef(hash, key) => self.get_chunk_by_hkey(
                &(self.get_chunk_by_hkey(&(hash.clone(), key.clone()).into(), compressor)?)
                    .data_ref()
                    .into(),
                compressor,
            )?,
            Hkey::List(list) => {
                // Parallel fetching of chunks
                let chunks: Vec<Result<DataChunk>> = list
                    .into_par_iter()
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

                chunk.into()
            }
        };

        Ok(chunk.into())
    }
}
