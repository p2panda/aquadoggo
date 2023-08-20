// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use log::{debug, info};
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

    /// Optional list of whitelisted schema ids. When set, only these schema ids will be accepted
    /// on this node, if not set _all_ schema ids are accepted.
    supported_schema_ids: Option<Vec<SchemaId>>,

    /// Sender for broadcast channel informing subscribers about updated schemas.
    tx: Sender<SchemaId>,
}

impl SchemaProvider {
    /// Returns a `SchemaProvider` containing the given application schemas and all system schemas.
    pub fn new(
        application_schemas: Vec<Schema>,
        supported_schema_ids: Option<Vec<SchemaId>>,
    ) -> Self {
        // Collect all system and application schemas.
        let mut schemas = SYSTEM_SCHEMAS.clone();
        schemas.extend(&application_schemas);

        // Build hash map from schemas for fast lookup.
        let mut index = HashMap::new();
        for schema in schemas {
            index.insert(schema.id().to_owned(), schema.to_owned());
        }

        if let Some(supported_schema_ids) = &supported_schema_ids {
            index.retain(|schema_id, _| supported_schema_ids.contains(schema_id));
        };

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
            supported_schema_ids,
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
    /// Returns `true` if a schema was updated or it already existed in it's current state, and
    /// `false` if it was inserted.
    pub async fn update(&self, schema: Schema) -> Result<bool> {
        if let Some(supported_schema) = self.supported_schema_ids.as_ref() {
            if !supported_schema.contains(schema.id()) {
                bail!("Attempted to add unsupported schema to schema provider");
            }
        };

        let mut schemas = self.schemas.lock().await;
        let schema_exists = schemas.get(schema.id()).is_some();

        if schema_exists {
            // Return true here as the schema already exists in it's current state so we don't need
            // to mutate the schema store or announce any change.
            return Ok(true);
        }

        info!("Updating {}", schema.id().display());
        let is_update = schemas
            .insert(schema.id().clone(), schema.clone())
            .is_some();

        // Inform subscribers about new schema
        if self.tx.send(schema.id().to_owned()).is_err() {
            debug!("No subscriber has been informed about inserted / updated schema");
        }

        Ok(is_update)
    }

    /// Returns a list of all supported schema ids.
    ///
    /// If no whitelist was set it returns the list of all currently known schema ids. If a
    /// whitelist was set it directly returns the list itself.
    pub async fn supported_schema_ids(&self) -> Vec<SchemaId> {
        match &self.supported_schema_ids {
            Some(schema_ids) => schema_ids.clone(),
            None => self
                .all()
                .await
                .iter()
                .map(|schema| schema.id().to_owned())
                .collect(),
        }
    }

    /// Returns true if a whitelist of supported schema ids was provided through user
    /// configuration.
    pub fn is_whitelist_active(&self) -> bool {
        self.supported_schema_ids.is_some()
    }
}

impl Default for SchemaProvider {
    fn default() -> Self {
        Self::new(Vec::new(), None)
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
        let provider = SchemaProvider::new(vec![], Some(vec![new_schema_id.clone()]));
        let result = provider.update(new_schema).await;
        assert!(result.is_ok());
        assert!(!result.unwrap());

        assert!(provider.get(&new_schema_id).await.is_some());
    }

    #[tokio::test]
    async fn update_unsupported_schemas() {
        let provider = SchemaProvider::new(vec![], Some(vec![]));
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
