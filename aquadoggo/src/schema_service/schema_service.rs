// SPDX-License-Identifier: AGPL-3.0-or-later

// @TODO: The API changed here in `p2panda_rs`
// use p2panda_rs::schema::system::get_system_schema;
use p2panda_rs::schema::{Schema, SchemaId};
use tokio::sync::broadcast::{channel, Receiver, Sender};

use crate::db::traits::SchemaStore;
use crate::errors::SchemaServiceError;
use crate::SqlStorage;

#[derive(Clone, Debug)]
pub struct SchemaService {
    store: SqlStorage,
    tx: Sender<SchemaId>,
}

impl SchemaService {
    /// Initializes a new `SchemaService` using a provided [`SqlStorage`].
    pub fn new(store: SqlStorage) -> Self {
        let (tx, _) = channel(64);
        Self { store, tx }
    }

    pub fn on_schema_added(&self) -> Receiver<SchemaId> {
        self.tx.subscribe()
    }

    /// Retrieve a schema that may be a system or application schema by its schema id.
    pub async fn get_schema(
        &self,
        schema_id: SchemaId,
    ) -> Result<Option<Schema>, SchemaServiceError> {
        match schema_id {
            SchemaId::Application(_, _) => self.get_application_schema(schema_id).await,
            _ => Ok(Some(Self::get_system_schema(schema_id)?)),
        }
    }

    /// Returns all system and application schemas.
    pub async fn all_schemas(&self) -> Result<Vec<Schema>, SchemaServiceError> {
        let mut schemas = Self::all_system_schemas();
        schemas.append(&mut self.all_application_schemas().await?);
        Ok(schemas)
    }

    /// Registers a new schema in the provider.
    pub async fn update(&self, schema: &Schema) -> Result<(), SchemaServiceError> {
        // @TODO: Add it to the store

        // Inform subscribers about new schema
        if self.tx.send(schema.id().to_owned()).is_err() {
            // Do nothing here, as we don't mind if there are no subscribers
        }

        Ok(())
    }

    /// Enumerate all known application schemas.
    pub async fn all_application_schemas(&self) -> Result<Vec<Schema>, SchemaServiceError> {
        Ok(self.store.get_all_schema().await.unwrap())
    }

    /// Retrieve a specific application schema by its schema id.
    pub async fn get_application_schema(
        &self,
        schema_id: SchemaId,
    ) -> Result<Option<Schema>, SchemaServiceError> {
        let view_id = match schema_id {
            SchemaId::Application(_, view_id) => Ok(view_id),
            _ => Err(SchemaServiceError::InvalidSchema(
                schema_id,
                "requires an application schema".to_string(),
            )),
        }?;

        Ok(self.store.get_schema_by_id(&view_id).await.unwrap())
    }

    /// Returns all known system schemas.
    pub fn all_system_schemas() -> Vec<Schema> {
        // @TODO: The API changed here in `p2panda_rs`
        /* let system_schemas = vec![
            SchemaId::SchemaDefinition(1),
            SchemaId::SchemaFieldDefinition(1),
        ]
        .iter()
        // Unwrap because tests make sure this works.
        .map(|schema_id| get_system_schema(schema_id.to_owned()).unwrap())
        .collect();
        system_schemas */
        Vec::new()
    }

    /// Retrieve a system schema by its schema id.
    pub fn get_system_schema(schema_id: SchemaId) -> Result<Schema, SchemaServiceError> {
        // @TODO: The API changed here in `p2panda_rs`
        // Ok(get_system_schema(schema_id)?)
        unimplemented!();
    }
}
