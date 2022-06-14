// SPDX-License-Identifier: AGPL-3.0-or-later

use async_trait::async_trait;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::schema::Schema;

use crate::db::errors::SchemaStoreError;

#[async_trait]
pub trait SchemaStore {
    /// Get a published Schema from storage by it's document view id.
    ///
    /// Returns a Schema or None if no schema was found with this document view id. Returns
    /// an error if a fatal storage error occured.
    async fn get_schema_by_id(
        &self,
        id: &DocumentViewId,
    ) -> Result<Option<Schema>, SchemaStoreError>;

    /// Get all published Schema from storage.
    ///
    /// Returns a vector of Schema, or an empty vector if none were found. Returns
    /// an error when a fatal storage error occured or a schema could not be constructed.
    async fn get_all_schema(&self) -> Result<Vec<Schema>, SchemaStoreError>;
}
