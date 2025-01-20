use std::marker::PhantomData;

use ps_mmap::ReadGuard;

use super::{DataStore, DataStoreHeader, DataStoreIndex, DataStorePager};

#[derive(Debug)]
pub(crate) struct DataStoreReadGuard<'lt> {
    inner: ReadGuard,
    _data: PhantomData<&'lt [u8]>,
}

impl<'lt> DataStoreReadGuard<'lt> {
    #[inline]
    pub fn get_header(&self) -> &'lt DataStoreHeader {
        unsafe { &*(self.inner.as_ptr() as *const DataStoreHeader) }
    }

    #[inline]
    pub fn get_index(&self) -> &'lt DataStoreIndex<'lt> {
        unsafe {
            DataStoreIndex::at_offset(self.inner.as_ptr(), self.get_header().index_offset as usize)
        }
    }

    #[inline]
    pub fn get_pager(&self) -> &'lt DataStorePager<'lt> {
        unsafe {
            DataStorePager::at_offset(self.inner.as_ptr(), self.get_header().data_offset as usize)
        }
    }
}

impl<'lt> From<&DataStore<'lt>> for DataStoreReadGuard<'lt> {
    fn from(value: &DataStore) -> Self {
        Self {
            inner: value.mmap.read(),
            _data: PhantomData,
        }
    }
}
