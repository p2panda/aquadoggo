// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use log::{debug, info, trace};
use p2panda_rs::schema::{Schema, SchemaId, SYSTEM_SCHEMAS};
use p2panda_rs::Human;
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio::sync::Mutex;

use crate::config::AllowList;

/// Provides fast access to system and application schemas.
///
/// Application schemas can be added and updated.
#[derive(Clone, Debug)]
pub struct SchemaProvider {
    /// In-memory store of registered and materialized schemas.
    schemas: Arc<Mutex<HashMap<SchemaId, Schema>>>,

    /// Optional list of allowed schema ids. When not empty, only these schema ids will be accepted
    /// on this node, if not set _all_ schema ids are accepted (wildcard).
    supported_schema_ids: AllowList<SchemaId>,

    /// Sender for broadcast channel informing subscribers about updated schemas.
    tx: Sender<SchemaId>,
}

impl SchemaProvider {
    /// Returns a `SchemaProvider` containing the given application schemas and all system schemas.
    pub fn new(
        application_schemas: Vec<Schema>,
        supported_schema_ids: AllowList<SchemaId>,
    ) -> Self {
        // Collect all system and application schemas.
        let mut schemas = SYSTEM_SCHEMAS.clone();
        schemas.extend(&application_schemas);

        // Build hash map from schemas for fast lookup.
        let mut index = HashMap::new();
        for schema in schemas {
            index.insert(schema.id().to_owned(), schema.to_owned());
        }

        // Filter out all unsupported schema ids when list was set
        if let AllowList::Set(schema_ids) = &supported_schema_ids {
            index.retain(|id, _| schema_ids.contains(id));
        };

        let (tx, _) = channel(64);

        trace!(
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
        if let AllowList::Set(supported_schema_ids) = &self.supported_schema_ids {
            if !supported_schema_ids.contains(schema.id()) {
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
    /// If no allow-list was set it returns the list of all currently known schema ids. If an
    /// allo-wlist was set it directly returns the list itself.
    pub async fn supported_schema_ids(&self) -> Vec<SchemaId> {
        match &self.supported_schema_ids {
            AllowList::Set(schema_ids) => schema_ids.clone(),
            AllowList::Wildcard => self
                .all()
                .await
                .iter()
                .map(|schema| schema.id().to_owned())
                .collect(),
        }
    }

    /// Returns true if an allow-list of supported schema ids was provided through user
    /// configuration.
    pub fn is_allow_list_active(&self) -> bool {
        matches!(self.supported_schema_ids, AllowList::Set(_))
    }
}

impl Default for SchemaProvider {
    fn default() -> Self {
        Self::new(Vec::new(), AllowList::Wildcard)
    }
}

#[cfg(test)]
mod test {
    use p2panda_rs::schema::{FieldType, Schema, SchemaId, SchemaName};
    use p2panda_rs::test_utils::fixtures::random_document_view_id;

    use crate::AllowList;

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
        let provider = SchemaProvider::new(vec![], AllowList::Set(vec![new_schema_id.clone()]));
        let result = provider.update(new_schema).await;
        assert!(result.is_ok());
        assert!(!result.unwrap());

        assert!(provider.get(&new_schema_id).await.is_some());
    }

    #[tokio::test]
    async fn update_unsupported_schemas() {
        let provider = SchemaProvider::new(vec![], AllowList::Set(vec![]));
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
