// SPDX-License-Identifier: AGPL-3.0-or-later

use async_trait::async_trait;
use p2panda_rs::schema::{Schema, SchemaId};

/// `SchemaStore` errors.
#[derive(thiserror::Error, Debug)]
pub enum SchemaStoreError {
    /// Catch all error which implementers can use for passing their own errors up the chain.
    #[error("Error occured in DocumentStore: {0}")]
    Custom(String),
}

#[async_trait]
pub trait SchemaStore {
    async fn insert_schema(&self, schema: &Schema) -> Result<(), SchemaStoreError>;

    async fn get_schema_by_id(&self, id: &SchemaId) -> Result<Schema, SchemaStoreError>;
}
