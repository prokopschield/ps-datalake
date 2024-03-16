use std::error::Error;

use chacha20poly1305::aead::{Aead, KeyInit};

use super::compressor::Compressor;

pub struct Encrypted {
    pub bytes: Vec<u8>,
    pub hash: String,
    pub key: String,
}

const KSIZE: usize = 32;
const NSIZE: usize = 12;

pub fn parse_key(key: &[u8]) -> ([u8; KSIZE], [u8; NSIZE]) {
    let raw_key = ps_base64::decode(key);

    let mut encryption_key = [0u8; KSIZE];
    let mut nonce = [0u8; NSIZE];

    encryption_key.copy_from_slice(&raw_key[0..KSIZE]);
    nonce.copy_from_slice(&raw_key[raw_key.len() - NSIZE..raw_key.len()]);

    return (encryption_key, nonce);
}

pub fn encrypt(data: &[u8], compressor: &Compressor) -> Result<Encrypted, Box<dyn Error>> {
    let compressed_data = compressor.compress(data)?;
    let hash_of_raw_data = ps_hash::hash(data);
    let (encryption_key, nonce) = parse_key(&hash_of_raw_data.as_bytes());
    let chacha = chacha20poly1305::ChaCha12Poly1305::new(&encryption_key.into());
    let encrypted_data = chacha.encrypt(&nonce.into(), compressed_data.as_ref())?;
    let hash_of_encrypted_data = ps_hash::hash(&encrypted_data);

    Ok(Encrypted {
        bytes: encrypted_data,
        hash: hash_of_encrypted_data,
        key: hash_of_raw_data,
    })
}

pub fn decrypt(
    data: &[u8],
    key: &[u8],
    compressor: &Compressor,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let (encryption_key, nonce) = parse_key(key);
    let chacha = chacha20poly1305::ChaCha12Poly1305::new(&encryption_key.into());
    let compressed_data = chacha.decrypt(&nonce.into(), data)?;

    let out_size_vec = ps_base64::decode(&key[36..38]);
    let out_size = ((out_size_vec[0] as usize) << 8) + (out_size_vec[1] as usize);

    Ok(compressor.decompress(&compressed_data, out_size)?)
}
