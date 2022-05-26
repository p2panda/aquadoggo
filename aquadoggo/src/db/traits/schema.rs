// SPDX-License-Identifier: AGPL-3.0-or-later

use async_trait::async_trait;

use p2panda_rs::document::DocumentViewId;
use p2panda_rs::schema::Schema;

use crate::db::errors::SchemaStoreError;

#[async_trait]
pub trait SchemaStore {
    async fn get_schema_by_id(&self, id: &DocumentViewId) -> Result<Schema, SchemaStoreError>;

    async fn get_all_schema(&self) -> Result<Vec<Schema>, SchemaStoreError>;
}
