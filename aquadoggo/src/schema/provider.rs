// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use log::{info, warn};
use p2panda_rs::schema::{Schema, SchemaId, SYSTEM_SCHEMAS};
use tokio::sync::broadcast::{channel, Receiver, Sender};

/// Provides fast access to system and application schemas during runtime.
///
/// Schemas can be updated and removed.
#[derive(Clone, Debug)]
pub struct SchemaProvider {
    /// In-memory store of registered schemas.
    schemas: Arc<Mutex<HashMap<SchemaId, Schema>>>,

    /// Sender for broadcast channel informing subscribers about updated schemas.
    tx: Sender<SchemaId>,
}

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

        let (tx, _) = channel(64);

        Self {
            schemas: Arc::new(Mutex::new(index)),
            tx,
        }
    }

    /// Returns receiver for broadcast channel.
    ///
    ///
    pub fn on_schema_added(&self) -> Receiver<SchemaId> {
        self.tx.subscribe()
    }

    /// Retrieve a schema that may be a system or application schema by its schema id.
    pub fn get(&self, schema_id: &SchemaId) -> Option<Schema> {
        self.schemas.lock().unwrap().get(schema_id).cloned()
    }

    /// Returns all system and application schemas.
    pub fn all(&self) -> Vec<Schema> {
        self.schemas.lock().unwrap().values().cloned().collect()
    }

    /// Inserts or updates the given schema in this provider.
    ///
    /// Returns `true` if a schema was updated and `false` if it was inserted.
    pub fn update(&self, schema: Schema) -> bool {
        info!("Updating schema {}", schema);
        let mut schemas = self.schemas.lock().unwrap();
        let is_update = schemas
            .insert(schema.id().clone(), schema.clone())
            .is_some();

        // Inform subscribers about new schema
        if self.tx.send(schema.id().to_owned()).is_err() {
            warn!("No subscriber has been informed about inserted / updated schema");
        }

        is_update
    }

    /// Remove a schema from this provider.
    ///
    /// Returns true if the schema existed.
    pub fn remove(&self, schema_id: &SchemaId) -> bool {
        info!("Removing schema {}", schema_id);
        self.schemas.lock().unwrap().remove(schema_id).is_some()
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
        let result = schemas.all();
        assert_eq!(result.len(), 2);
    }
}
