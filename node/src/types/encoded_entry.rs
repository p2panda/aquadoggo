use serde::{Deserialize, Serialize};
use sqlx::Type;
use validator::{Validate, ValidationError, ValidationErrors};

use crate::errors::Result;

#[derive(Type, Clone, Debug, Serialize, Deserialize)]
#[sqlx(transparent)]
pub struct EncodedEntry(String);

impl EncodedEntry {
    /// Validates and returns an encoded entry instance when correct.
    #[allow(dead_code)]
    pub fn new(value: &str) -> Result<Self> {
        let encoded_entry = Self(String::from(value));
        encoded_entry.validate()?;
        Ok(encoded_entry)
    }
}

impl Validate for EncodedEntry {
    fn validate(&self) -> anyhow::Result<(), ValidationErrors> {
        let mut errors = ValidationErrors::new();

        // Check if encoded entry is hex encoded
        match hex::decode(self.0.to_owned()) {
            Ok(bytes) => {
                if bamboo_core::decode(&bytes).is_err() {
                    errors.add("encoded_entry", ValidationError::new("invalid bamboo entry encoding"));
                }
            }
            Err(_) => {
                errors.add("encoded_entry", ValidationError::new("invalid hex string"));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::EncodedEntry;

    #[test]
    fn validate() {
        // Invalid hex string
        assert!(EncodedEntry::new("123456789Z").is_err());

        // Invalid author key
        assert!(EncodedEntry::new("0040276296658c0b83a3ce287f72738da1c3f8adb5fb13b15467b54acdf59d01800010d004069db5208a271c53de8a1b6220e6a4d7fcccd89e6c0c7e75c833e34dc68d932624f2ccf27513f42fb7d0e4390a99b225bad41ba14a6297537246dbe4e6ce150e89dec54a9279e958e06b10d4648ed7178152d0c5e44ad14e371474f2d77ae2388fd29ae4196b05dd119ec4c43e0dcfc4132aabccb187b536c3fca3b12aad0db07").is_err());

        assert!(EncodedEntry::new("00ae2d4093a07917008ca6dbd9536490a02cefad8d2ee96512f70446390cfc833f00010d004069db5208a271c53de8a1b6220e6a4d7fcccd89e6c0c7e75c833e34dc68d932624f2ccf27513f42fb7d0e4390a99b225bad41ba14a6297537246dbe4e6ce150e8f24c755be79747a9eabb955974cf9c154373528c9da5f44932f7a907f367b08f9e798c80a8faad70d4c15953763583ef2b4aca8db0e5ea2e7db17bbea0d14c0e").is_ok());
    }
}
