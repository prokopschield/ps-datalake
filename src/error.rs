use std::sync::PoisonError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PsDataLakeError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    PsDataChunkError(#[from] ps_datachunk::PsDataChunkError),
    #[error(transparent)]
    PsHashError(#[from] ps_hash::PsHashError),
    #[error(transparent)]
    PsMmapError(#[from] ps_mmap::PsMmapError),
    #[error(transparent)]
    TomlSerError(#[from] Box<toml::ser::Error>),
    #[error(transparent)]
    TomlDeError(#[from] Box<toml::de::Error>),
    #[error("Index out of range")]
    RangeError,
    #[error("DataChunk not found")]
    NotFound,
    #[error("Index overflowed - too many index buckets")]
    IndexBucketOverflow,
    #[error("The store being written to is read-only")]
    DataStoreNotRw,
    #[error("DataStore is out of space!")]
    DataStoreOutOfSpace,
    #[error("DataLake is out of available stores!")]
    DataLakeOutOfStores,
    #[error("Failed to acquire a poisoned mutex")]
    MutexPoisonError,
    #[error("Failed to store data")]
    StorageFailure,
    #[error("Invalid input format")]
    FormatError,
}

pub type Result<T> = std::result::Result<T, PsDataLakeError>;

impl<T> From<PoisonError<T>> for PsDataLakeError {
    fn from(_: PoisonError<T>) -> Self {
        Self::MutexPoisonError
    }
}

impl From<toml::ser::Error> for PsDataLakeError {
    fn from(value: toml::ser::Error) -> Self {
        Self::TomlSerError(Box::from(value))
    }
}

impl From<toml::de::Error> for PsDataLakeError {
    fn from(value: toml::de::Error) -> Self {
        Self::TomlDeError(Box::from(value))
    }
}
