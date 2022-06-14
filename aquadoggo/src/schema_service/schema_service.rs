// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::schema::system::get_system_schema;
use p2panda_rs::schema::{Schema, SchemaId};
use p2panda_rs::storage_provider::traits::StorageProvider;

use crate::db::stores::{StorageEntry, StorageLog};
use crate::db::traits::SchemaStore;
use crate::errors::SchemaServiceError;

#[derive(Clone)]
pub struct SchemaService<Provider>(Provider)
where
    Provider: StorageProvider<StorageEntry, StorageLog> + SchemaStore + Clone;

impl<Provider: StorageProvider<StorageEntry, StorageLog> + SchemaStore + Clone>
    SchemaService<Provider>
{
    /// Initializes a new `SchemaService` using a provided [`StorageProvider`].
    pub fn new(store: Provider) -> Self {
        Self(store)
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

    /// Enumerate all known application schemas.
    pub async fn all_application_schemas(&self) -> Result<Vec<Schema>, SchemaServiceError> {
        Ok(self.0.get_all_schema().await.unwrap())
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
        Ok(self.0.get_schema_by_id(&view_id).await.unwrap())
    }

    /// Returns all known system schemas.
    pub fn all_system_schemas() -> Vec<Schema> {
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
    pub fn get_system_schema(schema_id: SchemaId) -> Result<Schema, SchemaServiceError> {
        Ok(get_system_schema(schema_id)?)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::stores::test_utils::test_db;

    #[tokio::test]
    async fn test_get_all_schemas() {
        let (storage_provider, _, _) = test_db(1, 1, false).await;
        let schema_service = SchemaService::new(storage_provider);
        let result = schema_service.all_schemas().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }
}
