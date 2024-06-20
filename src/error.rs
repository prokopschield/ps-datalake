use thiserror::Error;

#[derive(Error, Debug)]
pub enum PsDataLakeError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    PsDataChunkError(#[from] ps_datachunk::PsDataChunkError),
    #[error("Index out of range")]
    RangeError,
    #[error("DataChunk not found")]
    NotFound,
    #[error("Index overflowed - too many index buckets")]
    IndexBucketOverflow,
}
