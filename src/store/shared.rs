use std::marker::PhantomData;

use ps_mmap::ReadGuard;

use crate::error::{AlignmentError, OffsetError};

use super::{DataStore, DataStoreHeader, DataStoreIndex, DataStorePager};

#[derive(Debug)]
pub struct DataStoreReadGuard<'lt> {
    inner: ReadGuard,
    _data: PhantomData<&'lt [u8]>,
}

impl<'lt> DataStoreReadGuard<'lt> {
    #[inline]
    pub fn get_header(&self) -> Result<&'lt DataStoreHeader, AlignmentError> {
        let ptr: *const DataStoreHeader = self.inner.as_ptr().cast();

        if !ptr.is_aligned() {
            return Err(AlignmentError);
        }

        Ok(unsafe { &*ptr })
    }

    #[inline]
    pub fn get_index(&self) -> Result<&'lt DataStoreIndex<'lt>, OffsetError> {
        Ok(unsafe {
            DataStoreIndex::at_offset(
                self.inner.as_ptr(),
                usize::try_from(self.get_header()?.index_offset)?,
            )
        })
    }

    #[inline]
    pub fn get_pager(&self) -> Result<&'lt DataStorePager<'lt>, OffsetError> {
        Ok(unsafe {
            DataStorePager::at_offset(
                self.inner.as_ptr(),
                usize::try_from(self.get_header()?.data_offset)?,
            )
        })
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
