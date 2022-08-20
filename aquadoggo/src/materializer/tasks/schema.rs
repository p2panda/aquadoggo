// SPDX-License-Identifier: AGPL-3.0-or-later

use log::debug;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::DocumentStore;

use crate::context::Context;
use crate::db::traits::SchemaStore;
use crate::materializer::worker::{TaskError, TaskResult};
use crate::materializer::TaskInput;

/// A schema task assembles and stores schemas from their views.
///
/// Schema tasks are dispatched whenever a schema definition or schema field definition document
/// has all its immediate dependencies available in the store. It collects all required views for
/// the schema, instantiates it and adds it to the schema provider.
pub async fn schema_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    debug!("Working on {}", input);

    let input_view_id = match (input.document_id, input.document_view_id) {
        (None, Some(view_id)) => Ok(view_id),
        // The task input must contain only a view id.
        (_, _) => Err(TaskError::Critical("Invalid task input".into())),
    }?;

    // Determine the schema of the updated view id.
    let schema = context
        .store
        .get_schema_by_document_view(&input_view_id)
        .await
        .map_err(|err| TaskError::Critical(err.to_string()))?
        .unwrap();

    let updated_schema_definitions: Vec<DocumentViewId> = match schema {
        // This task is about an updated schema definition document so we only handle that.
        SchemaId::SchemaDefinition(_) => Ok(vec![input_view_id.clone()]),

        // This task is about an updated schema field definition document that may be used by
        // multiple schema definition documents so we must handle all of those.
        SchemaId::SchemaFieldDefinition(_) => {
            get_related_schema_definitions(&input_view_id, &context).await
        }
        _ => Err(TaskError::Critical(format!(
            "Unknown system schema id: {}",
            schema
        ))),
    }?;

    // The related schema definitions are not known yet to this node so we mark this task failed.
    if updated_schema_definitions.is_empty() {
        return Err(TaskError::Failure(
            "Related schema definition not given (yet)".into(),
        ));
    }

    for view_id in updated_schema_definitions.iter() {
        match context
            .store
            .get_schema_by_id(view_id)
            .await
            .map_err(|err| TaskError::Critical(err.to_string()))?
        {
            // Updated schema was assembled successfully and is now passed to schema provider.
            Some(schema) => {
                context.schema_provider.update(schema.clone()).await;
            }
            // This schema was not ready to be assembled after all so it is ignored.
            None => {
                debug!("Not yet ready to build schema for {}", view_id)
            }
        };
    }

    Ok(None)
}

/// Retrieve schema definitions that use the targeted schema field definition as one of their
/// fields.
async fn get_related_schema_definitions(
    target_field_definition: &DocumentViewId,
    context: &Context,
) -> Result<Vec<DocumentViewId>, TaskError> {
    // Retrieve all schema definition documents from the store
    let schema_definitions = context
        .store
        .get_documents_by_schema(&SchemaId::SchemaDefinition(1))
        .await
        .map_err(|err| TaskError::Critical(err.to_string()))
        .unwrap();

    // Collect all schema definitions that use the targeted field definition
    let mut related_schema_definitions = vec![];
    for schema in schema_definitions {
        let fields_value = schema.fields().get("fields").unwrap().value();

        if let OperationValue::PinnedRelationList(fields) = fields_value {
            if fields
                .iter()
                .any(|field_view_id| field_view_id == target_field_definition)
            {
                related_schema_definitions.push(schema.id().clone())
            } else {
                continue;
            }
        } else {
            // Abort if there are schema definitions in the store that don't match the schema
            // definition schema.
            Err(TaskError::Critical(
                "Schema definition operation does not have a 'fields' operation field".into(),
            ))?
        }
    }

    Ok(related_schema_definitions)
}

#[cfg(test)]
mod tests {
    use log::debug;
    use p2panda_rs::document::{DocumentId, DocumentViewId};
    use p2panda_rs::entry::traits::AsEncodedEntry;
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::{OperationBuilder, OperationValue, PinnedRelationList};
    use p2panda_rs::schema::{FieldType, Schema, SchemaId};
    use p2panda_rs::storage_provider::traits::DocumentStore;
    use p2panda_rs::test_utils::db::test_db::send_to_store;
    use rstest::rstest;

    use crate::context::Context;
    use crate::db::stores::test_utils::{test_db, TestDatabase, TestDatabaseRunner};
    use crate::materializer::tasks::reduce_task;
    use crate::materializer::TaskInput;

    use super::schema_task;

    /// Insert a test schema definition and schema field definition and run reduce tasks for both.
    async fn create_schema_documents(
        context: &Context,
        db: &TestDatabase,
    ) -> (DocumentViewId, DocumentViewId) {
        // Create field definition
        let create_field_definition = OperationBuilder::new(&SchemaId::SchemaFieldDefinition(1))
            .fields(&[
                ("name", OperationValue::String("field_name".to_string())),
                ("type", FieldType::String.into()),
            ])
            .build()
            .unwrap();

        let (entry_signed, _) = send_to_store(
            &db.store,
            &create_field_definition,
            Schema::get_system(SchemaId::SchemaFieldDefinition(1)).unwrap(),
            &KeyPair::new(),
        )
        .await
        .unwrap();
        let field_definition_id: DocumentId = entry_signed.hash().into();

        let input = TaskInput::new(Some(field_definition_id.clone()), None);
        reduce_task(context.clone(), input).await.unwrap();
        let field_view_id = db
            .store
            .get_document_by_id(&field_definition_id)
            .await
            .unwrap()
            .unwrap()
            .id()
            .to_owned();
        debug!("Created field definition {}", &field_view_id);

        // Create schema definition
        let create_schema_definition = OperationBuilder::new(&SchemaId::SchemaDefinition(1))
            .fields(&[
                ("name", OperationValue::String("schema_name".to_string())),
                (
                    "description",
                    OperationValue::String("description".to_string()),
                ),
                (
                    "fields",
                    OperationValue::PinnedRelationList(PinnedRelationList::new(vec![
                        field_view_id.clone(),
                    ])),
                ),
            ])
            .build()
            .unwrap();

        let (entry_signed, _) = send_to_store(
            &db.store,
            &create_schema_definition,
            Schema::get_system(SchemaId::SchemaDefinition(1)).unwrap(),
            &KeyPair::new(),
        )
        .await
        .unwrap();

        let schema_definition_id: DocumentId = entry_signed.hash().into();

        let input = TaskInput::new(Some(schema_definition_id.clone()), None);
        reduce_task(context.clone(), input).await.unwrap();
        let definition_view_id = db
            .store
            .get_document_by_id(&schema_definition_id)
            .await
            .unwrap()
            .unwrap()
            .id()
            .to_owned();
        debug!("Created schema definition {}", definition_view_id);

        (definition_view_id, field_view_id)
    }

    #[rstest]
    fn assembles_schema(
        #[from(test_db)]
        #[with(1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            // Prepare schema definition and schema field definition
            let (definition_view_id, field_view_id) =
                create_schema_documents(&db.context, &db).await;

            // Start a task with each as input
            let input = TaskInput::new(None, Some(definition_view_id.clone()));
            assert!(schema_task(db.context.clone(), input).await.is_ok());

            let input = TaskInput::new(None, Some(field_view_id));
            assert!(schema_task(db.context.clone(), input).await.is_ok());

            // The new schema should be available on storage provider.
            let schema = db
                .context
                .schema_provider
                .get(&SchemaId::Application(
                    "schema_name".to_string(),
                    definition_view_id.clone(),
                ))
                .await;
            assert!(schema.is_some());
            assert_eq!(
                schema
                    .unwrap()
                    .fields()
                    .get("field_name")
                    .unwrap()
                    .to_owned(),
                FieldType::String
            );
        });
    }
}
