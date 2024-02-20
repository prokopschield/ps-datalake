use std::error::Error;

use chacha20poly1305::aead::{Aead, KeyInit};

use super::compressor::Compressor;

pub struct Encrypted {
    pub bytes: Vec<u8>,
    pub hash: String,
    pub key: String,
}

pub fn encrypt(data: &[u8], compressor: Compressor) -> Result<Encrypted, Box<dyn Error>> {
    let compressed_data = compressor.compress(data)?;
    let hash_of_raw_data = ps_hash::hash(data);
    let binary_hash_of_raw_data = ps_base64::decode(hash_of_raw_data.as_bytes());
    let encryption_key = &binary_hash_of_raw_data[0..32];
    let nonce = &binary_hash_of_raw_data[32..35];
    let chacha = chacha20poly1305::ChaCha12Poly1305::new(encryption_key.into());
    let encrypted_data = chacha.encrypt(nonce.into(), compressed_data.as_ref())?;
    let hash_of_encrypted_data = ps_hash::hash(&encrypted_data);

    Ok(Encrypted {
        bytes: encrypted_data,
        hash: hash_of_encrypted_data,
        key: hash_of_raw_data,
    })
}
