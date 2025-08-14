use crate::error::Result;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Default, Deserialize, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct ConfigStoreEntry {
    pub filename: String,
    pub readonly: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct DataLakeConfig {
    pub stores: Vec<ConfigStoreEntry>,
}

impl DataLakeConfig {
    pub fn from_toml_str(config: &str) -> Result<Self> {
        Ok(toml::de::from_str::<Self>(config)?)
    }

    pub fn to_toml_string(&self) -> Result<String> {
        Ok(toml::ser::to_string_pretty(self)?)
    }
}
