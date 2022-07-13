// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;
use std::sync::Arc;

use log::info;
use p2panda_rs::schema::{Schema, SchemaId, SYSTEM_SCHEMAS};
use tokio::sync::Mutex;

/// Provides fast access to system and application schemas during runtime.
///
/// System schemas are built-in and can be accessed without creating a `SchemaProvider` instance.
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

    /// Remove a schema from this provider.
    ///
    /// Returns true if the schema existed.
    pub async fn remove(&self, schema_id: &SchemaId) -> bool {
        info!("Removing {}", schema_id);
        self.0.lock().await.remove(schema_id).is_some()
    }
}

impl Default for SchemaProvider {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_get_all_schemas() {
        let schemas = SchemaProvider::default();
        let result = schemas.all().await;
        assert_eq!(result.len(), 2);
    }
}
