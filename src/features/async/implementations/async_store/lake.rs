use ps_datachunk::{DataChunk, OwnedDataChunk};
use ps_hash::Hash;
use ps_hkey::AsyncStore;
use ps_promise::Promise;

use crate::error::DataLakeError;
use crate::lake::DataLake;

impl AsyncStore for DataLake<'static> {
    type Chunk = OwnedDataChunk;
    type Error = DataLakeError;

    fn get(&self, hash: &Hash) -> Promise<Self::Chunk, Self::Error> {
        let this = self.clone();
        let hash = *hash;

        Promise::unblock(move || {
            let mut error = DataLakeError::NotFound;

            for store in &*this.stores.readable {
                match store.get_bucket_index_chunk_by_hash(&hash) {
                    Ok((_, _, Some(chunk))) => return Ok(chunk.into_owned()),
                    Ok(_) => (),
                    Err(err) => error = err,
                }
            }

            Err(error)
        })
    }

    fn put_verbatim<C: DataChunk>(&self, chunk: C) -> Promise<(), Self::Error> {
        let this = self.clone();
        let chunk = chunk.into_owned();

        Promise::unblock(move || this.put_opaque_chunk(&chunk))
    }
}
