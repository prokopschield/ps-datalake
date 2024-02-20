use std::{cell::RefCell, error::Error};

// 4096 (max item size) + 5 bytes (per libdeflater)
const COMPRESSION_VEC_CAPACITY: usize = 4111;

pub fn alloc_compressor() -> libdeflater::Compressor {
    let level = libdeflater::CompressionLvl::best();
    let compressor = libdeflater::Compressor::new(level);

    return compressor;
}

pub fn get_compressor(cell: &RefCell<Option<libdeflater::Compressor>>) -> libdeflater::Compressor {
    match cell.replace(None) {
        Some(compressor) => compressor,
        None => alloc_compressor(),
    }
}

pub fn put_compressor(
    cell: &RefCell<Option<libdeflater::Compressor>>,
    compressor: libdeflater::Compressor,
) {
    cell.replace(Some(compressor));
}

pub struct Compressor {
    cell: RefCell<Option<libdeflater::Compressor>>,
}

impl Compressor {
    pub fn new() -> Self {
        Self {
            cell: RefCell::from(Some(alloc_compressor())),
        }
    }

    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut compressor = get_compressor(&self.cell);
        let mut out_data = Vec::with_capacity(COMPRESSION_VEC_CAPACITY);

        unsafe { out_data.set_len(COMPRESSION_VEC_CAPACITY) };

        let result = compressor.deflate_compress(data, out_data.as_mut_slice());

        put_compressor(&self.cell, compressor);

        let size = result?;

        if size < COMPRESSION_VEC_CAPACITY {
            unsafe { out_data.set_len(size) };
        }

        Ok(out_data)
    }
}
