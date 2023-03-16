// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;
use std::sync::Arc;

use log::{debug, info, warn};
use p2panda_rs::schema::{Schema, SchemaId, SYSTEM_SCHEMAS};
use p2panda_rs::Human;
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio::sync::Mutex;

/// Provides fast thread-safe access to system and application schemas.
///
/// Application schemas can be added and updated.
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

        debug!(
            "Initialised schema provider:\n- {}",
            index
                .values()
                .map(|schema| schema.to_string())
                .collect::<Vec<String>>()
                .join("\n- ")
        );

        Self {
            schemas: Arc::new(Mutex::new(index)),
            tx,
        }
    }

    /// Returns receiver for broadcast channel.
    pub fn on_schema_added(&self) -> Receiver<SchemaId> {
        self.tx.subscribe()
    }

    /// Retrieve a schema that may be a system or application schema by its schema id.
    pub async fn get(&self, schema_id: &SchemaId) -> Option<Schema> {
        self.schemas.lock().await.get(schema_id).cloned()
    }

    /// Returns all system and application schemas.
    pub async fn all(&self) -> Vec<Schema> {
        self.schemas.lock().await.values().cloned().collect()
    }

    /// Inserts or updates the given schema in this provider.
    ///
    /// Returns `true` if a schema was updated and `false` if it was inserted.
    pub async fn update(&self, schema: Schema) -> bool {
        info!("Updating {}", schema.id().display());
        let mut schemas = self.schemas.lock().await;
        let is_update = schemas
            .insert(schema.id().clone(), schema.clone())
            .is_some();

        // Inform subscribers about new schema
        if self.tx.send(schema.id().to_owned()).is_err() {
            warn!("No subscriber has been informed about inserted / updated schema");
        }

        is_update
    }
}

impl Default for SchemaProvider {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

#[cfg(test)]
mod test {
    use p2panda_rs::schema::{FieldType, Schema, SchemaId, SchemaName};
    use p2panda_rs::test_utils::fixtures::random_document_view_id;

    use super::SchemaProvider;

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
        let new_schema_id = SchemaId::Application(
            SchemaName::new("test_schema").unwrap(),
            random_document_view_id(),
        );
        let new_schema = Schema::new(
            &new_schema_id,
            "description",
            &[("test_field", FieldType::String)],
        )
        .unwrap();
        let is_update = provider.update(new_schema).await;
        assert!(!is_update);

        assert!(provider.get(&new_schema_id).await.is_some());
    }
}
