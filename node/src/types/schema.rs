use serde::{Deserialize, Serialize};
use sqlx::Type;
use validator::{Validate, ValidationErrors};

use crate::errors::Result;
use crate::types::EntryHash;

/// Schemas are entry hashes pointing at an entry with a schema migration.
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
        self.0.validate()
    }
}
