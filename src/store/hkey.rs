use crate::error::PsDataLakeError;
use crate::error::Result;
use ps_hash::Hash;
use std::sync::Arc;

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum Hkey {
    /// The data contained in this variant is the value referenced
    Raw(Arc<[u8]>),
    /// The data contained in this variant can be decoded via ps_base64::decode()
    Base64(Arc<str>),
    /// The data shall be read directly from the DataStore
    Direct(Arc<Hash>),
    /// **HashKey**: The data shall be read via `.0` and decrypted via `.1`
    Encrypted(Arc<Hash>, Arc<Hash>),
    /// A reference to an Encrypted list
    ListRef(Arc<Hash>, Arc<Hash>),
    /// A list to be concatinated
    List(Arc<[Hkey]>),
}

impl Hkey {
    pub fn from_base64_slice(value: &[u8]) -> Self {
        match std::str::from_utf8(value) {
            Ok(str) => Self::Base64(str.into()),
            _ => Self::Raw(value.into()),
        }
    }

    pub fn try_parse_direct(hash: &[u8]) -> Result<Arc<Hash>> {
        Ok(Hash::try_from(hash)?.into())
    }

    pub fn try_as_direct(hash: &[u8]) -> Result<Self> {
        let hash = Self::try_parse_direct(hash)?;
        let hkey = Self::Direct(hash);

        Ok(hkey)
    }

    pub fn try_parse_encrypted(hashkey: &[u8]) -> Result<(Arc<Hash>, Arc<Hash>)> {
        let (hash, key) = hashkey.split_at(50);
        let hash = Hash::try_from(hash)?;
        let key = Hash::try_from(key)?;

        Ok((hash.into(), key.into()))
    }

    pub fn try_as_encrypted(hashkey: &[u8]) -> Result<Self> {
        let (hash, key) = Self::try_parse_encrypted(hashkey)?;
        let hkey = Self::Encrypted(hash, key);

        Ok(hkey)
    }

    pub fn try_as_list_ref(hashkey: &[u8]) -> Result<Self> {
        let (hash, key) = Self::try_parse_encrypted(hashkey)?;
        let hkey = Self::ListRef(hash, key);

        Ok(hkey)
    }

    pub fn try_as_list(list: &[u8]) -> Result<Self> {
        let last_index = list.len() - 1;
        let first = *list.get(0).ok_or(PsDataLakeError::FormatError)?;
        let last = *list.get(last_index).ok_or(PsDataLakeError::FormatError)?;
        let content = &list[1..last_index];

        if first != b'[' || last != b']' {
            Err(PsDataLakeError::FormatError)?
        }

        let parts = content.split(|c| *c == b',');
        let items = parts.map(Self::parse);
        let items: Vec<Hkey> = items.collect();
        let items: Arc<[Hkey]> = Arc::from(items.into_boxed_slice());
        let list = Self::List(items);

        Ok(list)
    }

    pub fn try_as_prefixed(value: &[u8]) -> Result<Self> {
        match value[0] {
            b'B' => Ok(Self::from_base64_slice(&value[1..])),
            b'D' => Self::try_as_direct(&value[1..]),
            b'E' => Self::try_as_encrypted(&value[1..]),
            b'L' => Self::try_as_list_ref(&value[1..]),
            b'[' => Self::try_as_list(&value),
            _ => Ok(Self::from_base64_slice(value)),
        }
    }

    pub fn try_as(value: &[u8]) -> Result<Self> {
        match value.len() {
            50 => Self::try_as_direct(value),
            100 => Self::try_as_encrypted(value),
            _ => Self::try_as_prefixed(value),
        }
    }

    pub fn parse(value: &[u8]) -> Self {
        match Self::try_as(value) {
            Ok(hkey) => hkey,
            _ => Self::from_base64_slice(value),
        }
    }
}

impl From<&Hkey> for String {
    fn from(value: &Hkey) -> Self {
        match value {
            Hkey::Raw(raw) => format!("B{}", ps_base64::encode(raw)),
            Hkey::Base64(base64) => format!("B{}", base64),
            Hkey::Direct(hash) => format!("D{}", hash),
            Hkey::Encrypted(hash, key) => format!("E{}{}", hash, key),
            Hkey::ListRef(hash, key) => format!("R{}{}", hash, key),
            Hkey::List(list) => format!(
                "[{}]",
                list.into_iter().fold(String::new(), |a, i| {
                    match a.len() {
                        0 => format!("{}", i),
                        _ => format!("{},{}", a, i),
                    }
                })
            ),
        }
    }
}

impl std::fmt::Display for Hkey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&String::from(self))
    }
}

impl From<&[u8]> for Hkey {
    fn from(value: &[u8]) -> Self {
        Self::parse(value)
    }
}

impl From<&str> for Hkey {
    fn from(value: &str) -> Self {
        value.as_bytes().into()
    }
}

impl From<Arc<Hash>> for Hkey {
    fn from(hash: Arc<Hash>) -> Self {
        Self::Direct(hash)
    }
}

impl From<Hash> for Hkey {
    fn from(hash: Hash) -> Self {
        Self::Direct(hash.into())
    }
}

impl From<&Hash> for Hkey {
    fn from(hash: &Hash) -> Self {
        Self::from(*hash)
    }
}

impl From<(Arc<Hash>, Arc<Hash>)> for Hkey {
    fn from((hash, key): (Arc<Hash>, Arc<Hash>)) -> Self {
        Self::Encrypted(hash, key)
    }
}

impl From<(Hash, Hash)> for Hkey {
    fn from((hash, key): (Hash, Hash)) -> Self {
        Self::Encrypted(hash.into(), key.into())
    }
}

impl From<(&Hash, &Hash)> for Hkey {
    fn from((hash, key): (&Hash, &Hash)) -> Self {
        Self::from((*hash, *key))
    }
}
