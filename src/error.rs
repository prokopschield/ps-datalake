use std::{fmt::Display, num::TryFromIntError, sync::PoisonError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PsDataLakeError {
    #[error(transparent)]
    AlignmentError(#[from] AlignmentError),
    #[error(transparent)]
    DataStoreCorrupted(#[from] DataStoreCorrupted),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    MmapMapError(#[from] ps_mmap::MapError),
    #[error("Tried to get a RW-handle on a read-only store.")]
    MmapReadOnly,
    #[error(transparent)]
    Offset(#[from] OffsetError),
    #[error(transparent)]
    PsDataChunkError(#[from] ps_datachunk::PsDataChunkError),
    #[error(transparent)]
    PsHkeyError(#[from] ps_hkey::PsHkeyError),
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
    #[error("To initialize a DataStore, please provision at least 64 kB of space.")]
    InitFailedNotEnoughSpace(usize),
}

pub type Result<T> = std::result::Result<T, PsDataLakeError>;

#[derive(Error, Debug)]
pub struct AlignmentError;

impl Display for AlignmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DataStore is not memory-aligned properly.")
    }
}

#[derive(Error, Debug)]
pub enum OffsetError {
    #[error(transparent)]
    Alignment(#[from] AlignmentError),
    #[error("Integer conversion error")]
    TryFromInt(#[from] TryFromIntError),
}

impl<T> From<PoisonError<T>> for PsDataLakeError {
    fn from(_: PoisonError<T>) -> Self {
        Self::MutexPoisonError
    }
}

impl From<ps_mmap::DerefError> for PsDataLakeError {
    fn from(value: ps_mmap::DerefError) -> Self {
        match value {
            ps_mmap::DerefError::ReadOnly => Self::MmapReadOnly,
        }
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

#[derive(Clone, Debug, Error, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum DataStoreCorrupted {
    #[error("DataStore has a size of {0} bytes")]
    InvalidDataStoreSize(usize),
    #[error("DataStore has an invalid file signature: {0}")]
    InvalidMagic(String),
    #[error("Index offset out of bounds: {0} > {1}")]
    IndexOffsetOutOfBounds(u64, usize),
    #[error("Data offset out of bounds: {0} > {1}")]
    DataOffsetOutOfBounds(u64, usize),
    #[error("Index ends out of bounds: {0} > {1}")]
    IndexEndsOutOfBounds(usize, usize),
    #[error("Data ends out of bounds: {0} > {1}")]
    DataEndsOutOfBounds(usize, usize),
    #[error("Index overlaps with data: {0} > {1}")]
    IndexDataOverlap(usize, usize),
    #[error("Index modulo {0} > index length {1}")]
    IndexModuloTooSmall(u32, u32),
}
