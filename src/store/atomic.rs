use ps_mmap::{DerefError, WriteGuard};

use crate::error::{AlignmentError, OffsetError};

use super::{DataStore, DataStoreHeader, DataStoreIndex, DataStorePager};

#[derive(Debug)]
pub struct DataStoreWriteGuard {
    inner: WriteGuard,
}

impl DataStoreWriteGuard {
    #[inline]
    pub fn get_header(&mut self) -> Result<&mut DataStoreHeader, AlignmentError> {
        let ptr: *mut DataStoreHeader = self.inner.as_mut_ptr().cast();

        if !ptr.is_aligned() {
            return Err(AlignmentError);
        }

        Ok(unsafe { &mut *ptr })
    }

    #[inline]
    pub fn get_index(&mut self) -> Result<&mut DataStoreIndex<'_>, OffsetError> {
        Ok(unsafe {
            DataStoreIndex::at_offset_mut(
                self.inner.as_mut_ptr(),
                usize::try_from(self.get_header()?.index_offset)?,
            )
        })
    }

    #[inline]
    pub fn get_pager(&mut self) -> Result<&mut DataStorePager<'_>, OffsetError> {
        Ok(unsafe {
            DataStorePager::at_offset_mut(
                self.inner.as_mut_ptr(),
                usize::try_from(self.get_header()?.data_offset)?,
            )
        })
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
