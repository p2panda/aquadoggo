// SPDX-License-Identifier: AGPL-3.0-or-later

use async_trait::async_trait;
use p2panda_rs::schema::Schema;

#[async_trait]
pub trait SchemaStore {
    // async fn insert_schema(&self, schema: &Schema) -> Result<(), SchemaStoreError>;
}
