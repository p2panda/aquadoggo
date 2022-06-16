// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::schema::{SchemaId, SchemaIdError};

/// Errors returned by schema service.
#[derive(thiserror::Error, Debug)]
pub enum SchemaProviderError {
    /// Schema provider can only handle application schemas it has definitions for.
    #[error("not a known application schema: {0}")]
    UnknownApplicationSchema(SchemaId),

    /// This operation has a requirement on the schema parameter.
    #[error("invalid schema: {0}, {1}")]
    InvalidSchema(SchemaId, String),

    /// Schema provider can only handle valid schema ids.
    #[error(transparent)]
    InvalidSchemaId(#[from] SchemaIdError),
}
