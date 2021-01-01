use serde::{Deserialize, Serialize};
use sqlx::Type;
use validator::{Validate, ValidationErrors};

use crate::errors::Result;
use crate::types::EntryHash;

/// Schemas are pointers for entries describing how user data is formatted.
///
/// A schema addresses one entry via its hash. This entry holds the data to describe a schema.
#[derive(Type, Clone, Debug, Serialize, Deserialize)]
#[sqlx(transparent)]
pub struct Schema(EntryHash);

impl Schema {
    /// Validates and returns a schema when correct.
    #[allow(dead_code)]
    pub fn new(value: &str) -> Result<Self> {
        let hash = EntryHash::new(value)?;
        Ok(Self(hash))
    }
}

impl Validate for Schema {
    fn validate(&self) -> anyhow::Result<(), ValidationErrors> {
        // Only check if the inner entry hash is correct
        self.0.validate()
    }
}
