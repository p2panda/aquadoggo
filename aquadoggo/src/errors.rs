// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::schema::{SchemaId, SchemaIdError};
use thiserror::Error;

/// A result type used in aquadoggo modules.
pub type Result<T> = anyhow::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Errors returned by schema service.
#[derive(Error, Debug)]
pub enum SchemaProviderError {
    /// Schema service can only handle application schemas it has definitions for.
    #[allow(dead_code)]
    #[error("not a known application schema: {0}")]
    UnknownApplicationSchema(SchemaId),

    /// This operation has a requirement on the schema parameter.
    #[allow(dead_code)]
    #[error("invalid schema: {0}, {1}")]
    InvalidSchema(SchemaId, String),

    /// Schema service can only handle valid schema ids.
    #[error(transparent)]
    InvalidSchemaId(#[from] SchemaIdError),
}
