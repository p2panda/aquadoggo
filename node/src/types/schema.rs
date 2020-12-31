use serde::{Deserialize, Serialize};
use sqlx::Type;
use validator::{Validate, ValidationErrors};

use crate::errors::Result;

/// Schemas are entry hash strings pointing at an entry of a schema migration.
#[derive(Type, Clone, Debug, Serialize, Deserialize)]
#[sqlx(transparent)]
pub struct Schema(String);

impl Schema {
    /// Validates and returns a schema when correct.
    #[allow(dead_code)]
    pub fn new(value: &str) -> Result<Self> {
        let schema = Self(String::from(value));
        schema.validate()?;
        Ok(schema)
    }
}

impl Validate for Schema {
    fn validate(&self) -> anyhow::Result<(), ValidationErrors> {
        // @TODO
        Ok(())
    }
}
