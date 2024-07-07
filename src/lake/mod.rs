pub mod config;
use crate::store::DataStore;

#[derive(Clone, Default)]
pub struct DataLakeStores<'lt> {
    pub readable: Vec<DataStore<'lt>>,
    pub writable: Vec<DataStore<'lt>>,
}

pub struct DataLake<'lt> {
    pub stores: DataLakeStores<'lt>,
}
