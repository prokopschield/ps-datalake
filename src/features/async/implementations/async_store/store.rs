use ps_datachunk::{DataChunk, OwnedDataChunk};
use ps_hash::Hash;
use ps_hkey::AsyncStore;
use ps_promise::Promise;

use crate::error::DataLakeError;
use crate::store::DataStore;

impl AsyncStore for DataStore<'static> {
    type Chunk = OwnedDataChunk;
    type Error = DataLakeError;

    fn get(&self, hash: &Hash) -> Promise<Self::Chunk, Self::Error> {
        let this = self.clone();
        let hash = *hash;

        Promise::unblock(move || {
            let (_, _, chunk) = this.get_bucket_index_chunk_by_hash(&hash)?;

            Ok(chunk.ok_or(DataLakeError::NotFound)?.into_owned())
        })
    }

    fn put_verbatim<C: DataChunk>(&self, chunk: C) -> Promise<(), Self::Error> {
        let this = self.clone();
        let chunk = chunk.into_owned();

        Promise::unblock(move || {
            this.put_opaque_chunk(&chunk)?;

            Ok(())
        })
    }
}
