use serde::{Deserialize, Serialize};
use sqlx::Type;

use crate::errors::Result;

/// Schemas are entry hash strings pointing at an entry of a schema migration.
#[derive(Type, Clone, Debug, Serialize, Deserialize)]
#[sqlx(transparent)]
pub struct Schema(String);

impl Schema {
    /// Validates and returns a schema when correct.
    #[allow(dead_code)]
    pub fn new(inner: &str) -> Result<Self> {
        let entry = Self(String::from(inner));
        entry.validate()?;
        Ok(entry)
    }

    /// Checks if schema is valid.
    pub fn validate(&self) -> Result<()> {
        // @TODO
        Ok(())
    }
}
