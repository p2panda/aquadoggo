use ed25519_dalek::{PublicKey, PUBLIC_KEY_LENGTH};
use serde::{Deserialize, Serialize};
use sqlx::Type;

use crate::errors::{Error, Result};

/// Authors are hex encoded ed25519 public keys.
#[derive(Type, Clone, Debug, Serialize, Deserialize)]
#[sqlx(transparent)]
pub struct Author(String);

impl Author {
    /// Validates and returns an author when correct.
    #[allow(dead_code)]
    pub fn new(inner: &str) -> Result<Self> {
        let entry = Self(String::from(inner));
        entry.validate()?;
        Ok(entry)
    }

    /// Checks if author is valid.
    pub fn validate(&self) -> Result<()> {
        // Check if author is a hex string
        let bytes = hex::decode(self.0.as_str())
            .map_err(|_| Error::Validation("invalid `author` hex string.".to_owned()))?;

        // Check if length is correct
        if bytes.len() < PUBLIC_KEY_LENGTH {
            return Err(Error::Validation(
                "invalid `author` string length.".to_owned(),
            ));
        }

        // Check if ed25519 public key is valid
        PublicKey::from_bytes(&bytes)
            .map_err(|_| Error::Validation("invalid `author` public key.".to_owned()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Author;

    #[test]
    fn validate() {
        assert!(Author::new("abcdefg").is_err());
        assert!(Author::new("112233445566ff").is_err());
        assert!(
            Author::new("01234567812345678123456781234567812345678123456781234567812345678")
                .is_err()
        );
        assert!(
            Author::new("7cf4f58a2d89e93313f2de99604a814ecea9800cf217b140e9c3a7ba59a5d982").is_ok()
        );
    }
}
