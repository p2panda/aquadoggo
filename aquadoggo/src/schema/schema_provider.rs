// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;
use std::sync::Arc;

use log::info;
use p2panda_rs::schema::{Schema, SchemaId, SYSTEM_SCHEMAS};
use tokio::sync::Mutex;

/// Provides fast access to system and application schemas during runtime.
///
/// Schemas can be updated and removed.
#[derive(Clone, Debug)]
pub struct SchemaProvider(Arc<Mutex<HashMap<SchemaId, Schema>>>);

// Dead code allowed until this is used for https://github.com/p2panda/aquadoggo/pull/141
#[allow(dead_code)]
impl SchemaProvider {
    /// Returns a `SchemaProvider` containing the given application schemas and all system schemas.
    pub fn new(application_schemas: Vec<Schema>) -> Self {
        // Collect all system and application schemas.
        let mut schemas = SYSTEM_SCHEMAS.clone();
        schemas.extend(&application_schemas);

        // Build hash map from schemas for fast lookup.
        let mut index = HashMap::new();
        for schema in schemas {
            index.insert(schema.id().to_owned(), schema.to_owned());
        }
        Self(Arc::new(Mutex::new(index)))
    }

    /// Retrieve a schema that may be a system or application schema by its schema id.
    pub async fn get(&self, schema_id: &SchemaId) -> Option<Schema> {
        self.0.lock().await.get(schema_id).cloned()
    }

    /// Returns all system and application schemas.
    pub async fn all(&self) -> Vec<Schema> {
        self.0.lock().await.values().cloned().collect()
    }

    /// Inserts or updates the given schema in this provider.
    ///
    /// Returns `true` if a schema was updated and `false` if it was inserted.
    pub async fn update(&self, schema: Schema) -> bool {
        info!("Updating {}", schema);
        let mut schemas = self.0.lock().await;
        schemas.insert(schema.id().clone(), schema).is_some()
    }
}

impl Default for SchemaProvider {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

#[cfg(test)]
mod test {
    use p2panda_rs::schema::FieldType;
    use p2panda_rs::test_utils::fixtures::random_document_view_id;

    use super::*;

    #[tokio::test]
    async fn get_all_schemas() {
        let provider = SchemaProvider::default();
        let result = provider.all().await;
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn get_single_schema() {
        let provider = SchemaProvider::default();
        let schema_definition_schema = provider.get(&SchemaId::SchemaDefinition(1)).await;
        assert!(schema_definition_schema.is_some());
    }

    #[tokio::test]
    async fn update_schemas() {
        let provider = SchemaProvider::default();
        let new_schema_id =
            SchemaId::Application("test_schema".to_string(), random_document_view_id());
        let new_schema = Schema::new(
            &new_schema_id,
            "description",
            vec![("test_field", FieldType::String)],
        )
        .unwrap();
        let is_update = provider.update(new_schema).await;
        assert!(!is_update);

        assert!(provider.get(&new_schema_id).await.is_some());
    }
}
