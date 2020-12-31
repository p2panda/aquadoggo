use ed25519_dalek::{PublicKey, PUBLIC_KEY_LENGTH};
use serde::{Deserialize, Serialize};
use sqlx::Type;
use validator::{Validate, ValidationError, ValidationErrors};

use crate::errors::Result;

/// Authors are hex encoded ed25519 public keys.
#[derive(Type, Clone, Debug, Serialize, Deserialize)]
#[sqlx(transparent)]
pub struct Author(String);

impl Author {
    /// Validates and returns an author when correct.
    #[allow(dead_code)]
    pub fn new(value: &str) -> Result<Self> {
        let author = Self(String::from(value));
        author.validate()?;
        Ok(author)
    }
}

impl Validate for Author {
    fn validate(&self) -> anyhow::Result<(), ValidationErrors> {
        let mut errors = ValidationErrors::new();

        // Check if author is a hex string
        match hex::decode(self.0.to_owned()) {
            Ok(bytes) => {
                // Check if length is correct
                if bytes.len() < PUBLIC_KEY_LENGTH {
                    errors.add(
                        "author",
                        ValidationError::new("invalid `author` string length"),
                    );
                }

                // Check if ed25519 public key is valid
                if PublicKey::from_bytes(&bytes).is_err() {
                    errors.add(
                        "author",
                        ValidationError::new("invalid `author` ed25519 public key"),
                    );
                }
            }
            Err(_) => {
                errors.add(
                    "author",
                    ValidationError::new("invalid `author` hex string"),
                );
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
