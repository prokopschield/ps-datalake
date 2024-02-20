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

pub fn alloc_decompressor() -> libdeflater::Decompressor {
    libdeflater::Decompressor::new()
}

pub fn get_decompressor(
    cell: &RefCell<Option<libdeflater::Decompressor>>,
) -> libdeflater::Decompressor {
    match cell.replace(None) {
        Some(compressor) => compressor,
        None => alloc_decompressor(),
    }
}

pub fn put_decompressor(
    cell: &RefCell<Option<libdeflater::Decompressor>>,
    decompressor: libdeflater::Decompressor,
) {
    cell.replace(Some(decompressor));
}

pub struct Compressor {
    compressor: RefCell<Option<libdeflater::Compressor>>,
    decompressor: RefCell<Option<libdeflater::Decompressor>>,
}

impl Compressor {
    pub fn new() -> Self {
        Self {
            compressor: RefCell::from(Some(alloc_compressor())),
            decompressor: RefCell::from(Some(alloc_decompressor())),
        }
    }

    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut compressor = get_compressor(&self.compressor);
        let mut out_data = Vec::with_capacity(COMPRESSION_VEC_CAPACITY);

        unsafe { out_data.set_len(COMPRESSION_VEC_CAPACITY) };

        let result = compressor.deflate_compress(data, out_data.as_mut_slice());

        put_compressor(&self.compressor, compressor);

        let size = result?;

        if size < COMPRESSION_VEC_CAPACITY {
            unsafe { out_data.set_len(size) };
        }

        Ok(out_data)
    }

    pub fn decompress(&self, data: &[u8], out_size: usize) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut out_data = Vec::with_capacity(out_size);

        unsafe { out_data.set_len(out_size) };

        let mut decompressor = get_decompressor(&self.decompressor);

        let result = decompressor.deflate_decompress(data, out_data.as_mut_slice());

        put_decompressor(&self.decompressor, decompressor);

        let size = result?;

        if size < out_size {
            unsafe { out_data.set_len(size) };
        }

        Ok(out_data)
    }
}
