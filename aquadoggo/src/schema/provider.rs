// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use p2panda_rs::schema::system::get_system_schema;
use p2panda_rs::schema::{Schema, SchemaId, SchemaIdError};

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
        let mut schemas = Self::all_system();
        schemas.extend(application_schemas);

        // Build hash map from schemas for fast lookup.
        let mut index = HashMap::new();
        for schema in schemas {
            index.insert(schema.id().to_owned(), schema.to_owned());
        }
        Self(Arc::new(Mutex::new(index)))
    }

    /// Retrieve a schema that may be a system or application schema by its schema id.
    pub fn get(&self, schema_id: &SchemaId) -> Option<Schema> {
        self.0.lock().unwrap().get(schema_id).cloned()
    }

    /// Returns all system and application schemas.
    pub fn all(&self) -> Vec<Schema> {
        self.0.lock().unwrap().values().cloned().collect()
    }

    /// Returns all known system schemas.
    pub fn all_system() -> Vec<Schema> {
        let system_schemas = vec![
            SchemaId::SchemaDefinition(1),
            SchemaId::SchemaFieldDefinition(1),
        ]
        .iter()
        // Unwrap because tests make sure this works.
        .map(|schema_id| get_system_schema(schema_id.to_owned()).unwrap())
        .collect();
        system_schemas
    }

    /// Retrieve a system schema by its schema id.
    ///
    /// Returns an error if the `schema_id` parameter is not a system schema id.
    pub fn get_system(schema_id: SchemaId) -> Result<Schema, SchemaIdError> {
        get_system_schema(schema_id)
    }

    /// Inserts or updates the given schema in this provider.
    ///
    /// Returns `true` if a schema was updated and `false` if it was inserted.
    pub fn update(&self, schema: Schema) -> bool {
        let mut schemas = self.0.lock().unwrap();
        schemas.insert(schema.id().clone(), schema).is_some()
    }

    /// Remove a schema from this provider.
    ///
    /// Returns true if the schema existed.
    pub fn remove(&self, schema_id: &SchemaId) -> bool {
        self.0.lock().unwrap().remove(schema_id).is_some()
    }
}

impl Default for SchemaProvider {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

#[cfg(test)]
mod test {
    use rstest::rstest;

    use super::*;
    use crate::db::stores::test_utils::{test_db, TestSqlStore};
    use crate::db::traits::SchemaStore;

    #[tokio::test]
    async fn test_get_all_schemas() {
        let schemas = SchemaProvider::default();
        let result = schemas.all();
        assert_eq!(result.len(), 2);
    }
}
