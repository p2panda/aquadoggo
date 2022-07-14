// SPDX-License-Identifier: AGPL-3.0-or-later

use log::debug;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::operation::{AsOperation, AsVerifiedOperation, OperationValue};
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::OperationStore;

use crate::context::Context;
use crate::db::traits::{DocumentStore, SchemaStore};
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
    let schema = get_schema_for_view(&input_view_id, &context).await?;

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

/// Determine the schema of a view.
async fn get_schema_for_view(
    view_id: &DocumentViewId,
    context: &Context,
) -> Result<SchemaId, TaskError> {
    let sample_operation_id = view_id.graph_tips().first().unwrap();
    let sample_operation = context.store.get_operation_by_id(sample_operation_id).await;
    let sample_operation = sample_operation
        .map_err(|err| TaskError::Critical(err.to_string()))?
        .unwrap();

    Ok(sample_operation.operation().schema())
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
                .any(|field_view_id| &field_view_id == target_field_definition)
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
    use std::convert::TryFrom;

    use log::debug;
    use p2panda_rs::document::DocumentViewId;
    use p2panda_rs::entry::{sign_and_encode, Entry};
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::{Author, KeyPair};
    use p2panda_rs::operation::{
        AsVerifiedOperation, Operation, OperationEncoded, OperationFields, OperationValue,
        PinnedRelationList, VerifiedOperation,
    };
    use p2panda_rs::schema::{FieldType, SchemaId, SchemaVersion};
    use p2panda_rs::storage_provider::traits::{OperationStore, StorageProvider};
    use rstest::rstest;

    use crate::context::Context;
    use crate::db::request::{EntryArgsRequest, PublishEntryRequest};
    use crate::db::stores::test_utils::{test_db, TestDatabase, TestDatabaseRunner};
    use crate::materializer::tasks::reduce_task;
    use crate::materializer::TaskInput;
    use crate::schema::SchemaProvider;
    use crate::Configuration;

    use super::schema_task;

    /// Insert a test schema definition and schema field definition and fire reduce tasks for both.
    async fn insert_test_schema(
        context: &Context,
        key_pair: &KeyPair,
    ) -> (DocumentViewId, DocumentViewId) {
        let author = Author::try_from(key_pair.public_key().to_owned()).unwrap();

        // Publish schema field definition
        let args = EntryArgsRequest {
            public_key: author.clone(),
            document_id: None,
        };
        let args = context.store.get_entry_args(&args).await.unwrap();

        let mut fields = OperationFields::new();
        fields
            .add("name", OperationValue::Text("message".to_string()))
            .unwrap();
        fields.add("type", FieldType::String.into()).unwrap();
        let create_op = Operation::new_create(SchemaId::SchemaFieldDefinition(1), fields).unwrap();

        let entry = Entry::new(
            &args.log_id.into(),
            Some(&create_op),
            args.skiplink.map(Hash::from).as_ref(),
            args.backlink.map(Hash::from).as_ref(),
            &args.seq_num.into(),
        )
        .unwrap();
        let entry_encoded = sign_and_encode(&entry, key_pair).unwrap();
        let args = PublishEntryRequest {
            entry: entry_encoded.clone(),
            operation: OperationEncoded::try_from(&create_op).unwrap(),
        };
        let response = context.store.publish_entry(&args).await.unwrap();
        let field_view_id: DocumentViewId = response.backlink.unwrap().into();

        let verified_op =
            VerifiedOperation::new(&author, &entry_encoded.clone().hash().into(), &create_op)
                .unwrap();
        context
            .store
            .insert_operation(&verified_op, &entry_encoded.hash().into())
            .await
            .unwrap();

        let input = TaskInput::new(Some(entry_encoded.clone().hash().into()), None);
        reduce_task(context.clone(), input).await.unwrap();
        debug!("Created field definition {}", field_view_id);

        // Publish schema definition
        let args = EntryArgsRequest {
            public_key: author.clone(),
            document_id: None,
        };
        let args = context.store.get_entry_args(&args).await.unwrap();

        let mut fields = OperationFields::new();
        fields
            .add("name", OperationValue::Text("test_schema".to_string()))
            .unwrap();
        fields
            .add("description", OperationValue::Text("".to_string()))
            .unwrap();
        fields
            .add(
                "fields",
                OperationValue::PinnedRelationList(PinnedRelationList::new(vec![
                    field_view_id.clone()
                ])),
            )
            .unwrap();
        let create_op = Operation::new_create(SchemaId::SchemaDefinition(1), fields).unwrap();

        let entry = Entry::new(
            &args.log_id.into(),
            Some(&create_op),
            args.skiplink.map(Hash::from).as_ref(),
            args.backlink.map(Hash::from).as_ref(),
            &args.seq_num.into(),
        )
        .unwrap();
        let entry_encoded = sign_and_encode(&entry, key_pair).unwrap();
        let args = PublishEntryRequest {
            entry: entry_encoded.clone(),
            operation: OperationEncoded::try_from(&create_op).unwrap(),
        };
        let response = context.store.publish_entry(&args).await.unwrap();
        let definition_view_id = response.backlink.unwrap().into();

        let verified_op =
            VerifiedOperation::new(&author, &entry_encoded.clone().hash().into(), &create_op)
                .unwrap();
        context
            .store
            .insert_operation(&verified_op, &entry_encoded.hash().into())
            .await
            .unwrap();

        let input = TaskInput::new(Some(entry_encoded.clone().hash().into()), None);
        reduce_task(context.clone(), input).await.unwrap();
        debug!("Created schema definition {}", definition_view_id);

        return (definition_view_id, field_view_id);
    }

    #[rstest]
    fn assembles_schema(
        #[from(test_db)]
        #[with(1, 1)]
        runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            let context = Context::new(
                db.store.clone(),
                Configuration::default(),
                SchemaProvider::default(),
            );
            // Prepare schema definition and schema field definition
            let (definition_view_id, field_view_id) =
                insert_test_schema(&context, db.test_data.key_pairs.first().unwrap()).await;

            // Start a task with each as input
            let input = TaskInput::new(None, Some(definition_view_id.clone()));
            assert!(schema_task(context.clone(), input).await.is_ok());

            let input = TaskInput::new(None, Some(field_view_id));
            assert!(schema_task(context.clone(), input).await.is_ok());

            // The new schema should be available on storage provider.
            assert!(context
                .schema_provider
                .all()
                .await
                .iter()
                .any(|s| s.version() == SchemaVersion::Application(definition_view_id.clone())));
        });
    }
}
