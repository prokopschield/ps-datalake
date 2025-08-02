use ps_mmap::{DerefError, WriteGuard};

use super::{DataStore, DataStoreHeader, DataStoreIndex, DataStorePager};

#[derive(Debug)]
pub struct DataStoreWriteGuard {
    inner: WriteGuard,
}

impl DataStoreWriteGuard {
    #[inline]
    pub fn get_header(&mut self) -> &mut DataStoreHeader {
        unsafe { &mut *self.inner.as_mut_ptr().cast::<DataStoreHeader>() }
    }

    #[inline]
    pub fn get_index(&mut self) -> &mut DataStoreIndex {
        unsafe {
            DataStoreIndex::at_offset_mut(
                self.inner.as_mut_ptr(),
                self.get_header().index_offset as usize,
            )
        }
    }

    #[inline]
    pub fn get_pager(&mut self) -> &mut DataStorePager {
        unsafe {
            DataStorePager::at_offset_mut(
                self.inner.as_mut_ptr(),
                self.get_header().data_offset as usize,
            )
        }
    }
}

impl From<WriteGuard> for DataStoreWriteGuard {
    fn from(inner: WriteGuard) -> Self {
        Self { inner }
    }
}

impl TryFrom<&DataStore<'_>> for DataStoreWriteGuard {
    type Error = DerefError;

    fn try_from(value: &DataStore) -> Result<Self, Self::Error> {
        Ok(value.mmap.try_write()?.into())
    }
}
