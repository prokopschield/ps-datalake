use std::error::Error;
use std::fs::File;
use std::path::PathBuf;

use ps_datachunk::BorrowedDataChunk;
use ps_datachunk::DataChunk;
use ps_datalake::lake::config::ConfigStoreEntry;
use ps_datalake::lake::config::DataLakeConfig;
use ps_datalake::lake::DataLake;
use ps_datalake::store::DataStore;
use ps_hkey::Hkey;
use ps_hkey::Store;
use ps_hkey::MAX_DECRYPTED_SIZE;
use ps_hkey::MAX_SIZE_RAW;

type TestResult = Result<(), Box<dyn Error>>;

/// A store file that is created on construction and removed on drop.
struct StoreFile {
    path: PathBuf,
}

impl StoreFile {
    fn new(name: &str) -> Result<Self, Box<dyn Error>> {
        let path =
            std::env::temp_dir().join(format!("ps-datalake-{name}-{}.store", std::process::id()));

        File::create(&path)?.set_len(1 << 20)?;

        Ok(Self { path })
    }

    fn path_string(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }
}

impl Drop for StoreFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn open_store(name: &str) -> Result<(StoreFile, DataStore<'static>), Box<dyn Error>> {
    let file = StoreFile::new(name)?;
    let store = DataStore::init(&file.path)?;

    Ok((file, store))
}

fn open_lake(name: &str) -> Result<(StoreFile, DataLake<'static>), Box<dyn Error>> {
    let file = StoreFile::new(name)?;
    let config = DataLakeConfig {
        stores: vec![ConfigStoreEntry {
            filename: file.path_string(),
            readonly: false,
        }],
    };
    let lake = DataLake::init(config)?;

    Ok((file, lake))
}

/// Fills a buffer with xorshift output, which does not compress.
fn incompressible(len: usize) -> Vec<u8> {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;

    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;

            state.to_le_bytes()[0]
        })
        .collect()
}

/// Payloads that `put_verbatim` must store exactly as given: one small enough
/// to be inlined into an [`Hkey`], one that compresses well, and the encrypted
/// form of the largest payload that `put` stores as a single chunk.
fn payloads() -> Result<Vec<Vec<u8>>, Box<dyn Error>> {
    let small = incompressible(MAX_SIZE_RAW);
    let compressible = b"compressible ".repeat(160);
    let largest = incompressible(MAX_DECRYPTED_SIZE);
    let encrypted = BorrowedDataChunk::from_data(&largest)?.encrypt()?;

    Ok(vec![small, compressible, encrypted.data_ref().to_vec()])
}

/// Stores each payload verbatim and reads it back under its own hash.
fn assert_put_verbatim_round_trips<S: Store>(store: &S) -> TestResult
where
    S::Error: Error + 'static,
{
    for payload in payloads()? {
        let chunk = BorrowedDataChunk::from_data(&payload)?;

        store.put_verbatim(chunk.borrow())?;

        let stored = store.get(chunk.hash_ref())?;

        assert_eq!(stored.data_ref(), chunk.data_ref());
    }

    Ok(())
}

#[test]
fn store_put_verbatim_round_trips() -> TestResult {
    let (_file, store) = open_store("store-verbatim")?;

    assert_put_verbatim_round_trips(&store)
}

#[test]
fn lake_put_verbatim_round_trips() -> TestResult {
    let (_file, lake) = open_lake("lake-verbatim")?;

    assert_put_verbatim_round_trips(&lake)
}

#[test]
fn store_put_then_get_resolves_encrypted_hkey() -> TestResult {
    let (_file, store) = open_store("store-put")?;
    let data = incompressible(MAX_DECRYPTED_SIZE);

    let hkey = store.put(&data)?;

    let Hkey::Encrypted(hash, key) = hkey else {
        return Err(format!("expected an encrypted hkey, got {hkey:?}").into());
    };

    let decrypted = store.get(&hash)?.decrypt(&key)?;

    assert_eq!(decrypted.data_ref(), &data[..]);

    Ok(())
}
