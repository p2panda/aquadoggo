// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
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

    /// Optional list of schema this provider supports. If set only these schema will be added to the schema
    /// registry once materialized.
    supported_schema: Option<Vec<SchemaId>>,

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
            supported_schema: None,
            tx,
        }
    }

    pub fn new_with_supported_schema(supported_schema: Vec<SchemaId>) -> Self {
        // Validate that the passed known schema are all mentioned in the supported schema list.

        // Collect all system and application schemas.
        let system_schemas = SYSTEM_SCHEMAS.clone();

        // Filter system schema against passed supported schema and collect into index Hashmap.
        let index: HashMap<SchemaId, Schema> = system_schemas
            .into_iter()
            .filter_map(|schema| {
                if supported_schema.contains(schema.id()) {
                    Some((schema.id().to_owned(), schema.to_owned()))
                } else {
                    None
                }
            })
            .collect();

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
            supported_schema: Some(supported_schema),
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
    pub async fn update(&self, schema: Schema) -> Result<bool> {
        if let Some(supported_schema) = self.supported_schema.as_ref() {
            if !supported_schema.contains(schema.id()) {
                return Err(anyhow!(
                    "Attempted to add unsupported schema to schema provider"
                ));
            }
        };

        info!("Updating {}", schema.id().display());
        let mut schemas = self.schemas.lock().await;
        let is_update = schemas
            .insert(schema.id().clone(), schema.clone())
            .is_some();

        // Inform subscribers about new schema
        if self.tx.send(schema.id().to_owned()).is_err() {
            warn!("No subscriber has been informed about inserted / updated schema");
        }

        Ok(is_update)
    }

    pub fn supported_schema(&self) -> Vec<SchemaId> {
        self.supported_schema.clone().unwrap_or_default()
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
        let result = provider.update(new_schema).await;
        assert!(result.is_ok());
        assert!(!result.unwrap());

        assert!(provider.get(&new_schema_id).await.is_some());
    }

    #[tokio::test]
    async fn update_supported_schemas() {
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
        let provider = SchemaProvider::new_with_supported_schema(vec![new_schema_id.clone()]);
        let result = provider.update(new_schema).await;
        assert!(result.is_ok());
        assert!(!result.unwrap());

        assert!(provider.get(&new_schema_id).await.is_some());
    }

    #[tokio::test]
    async fn update_unsupported_schemas() {
        let provider = SchemaProvider::new_with_supported_schema(vec![]);
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
        let result = provider.update(new_schema).await;
        assert!(result.is_err());

        assert!(provider.get(&new_schema_id).await.is_none());
    }
}
