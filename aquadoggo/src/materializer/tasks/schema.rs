// SPDX-License-Identifier: AGPL-3.0-or-later

use log::debug;
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::DocumentStore;

use crate::context::Context;
use crate::materializer::worker::{TaskError, TaskResult};
use crate::materializer::TaskInput;

/// A schema task assembles and stores schemas from their views.
///
/// Schema tasks are dispatched whenever a schema definition or schema field definition document
/// has all its immediate dependencies available in the store. It collects all required views for
/// the schema, instantiates it and adds it to the schema provider.
pub async fn schema_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    debug!("Working on {}", input);

    let input_view_id = match input {
        TaskInput::SpecificView(view_id) => view_id,
        _ => return Err(TaskError::Critical("Invalid task input".into())),
    };

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
                match context.schema_provider.update(schema.clone()).await {
                    Ok(_) => (),
                    Err(err) => debug!("Schema not supported: {}", err),
                };
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
        // We can unwrap the value here as all documents returned from the storage method above
        // have a current view (they are not deleted).
        let fields_value = schema.get("fields").unwrap();

        if let OperationValue::PinnedRelationList(fields) = fields_value {
            if fields
                .iter()
                .any(|field_view_id| field_view_id == target_field_definition)
            {
                related_schema_definitions.push(schema.view_id().clone())
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
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::{OperationValue, PinnedRelationList};
    use p2panda_rs::schema::{FieldType, SchemaId, SchemaName};
    use p2panda_rs::test_utils::fixtures::key_pair;
    use rstest::rstest;

    use crate::materializer::TaskInput;
    use crate::test_utils::{add_document, test_runner, TestNode};

    use super::schema_task;

    #[rstest]
    fn assembles_schema(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            // Publish the schema fields document.
            //
            // This also runs a reduce and dependency task, materializing the document into the store.
            let field_view_id = add_document(
                &mut node,
                &SchemaId::SchemaFieldDefinition(1),
                vec![
                    ("name", OperationValue::String("field_name".to_string())),
                    ("type", FieldType::String.into()),
                ],
                &key_pair,
            )
            .await;

            // Publish the schema definition document.
            //
            // This also runs a reduce and dependency task, materializing the document into the store.
            let schema_view_id = add_document(
                &mut node,
                &SchemaId::SchemaDefinition(1),
                vec![
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
                ],
                &key_pair,
            )
            .await;

            // Run a schema task with each as input
            let input = TaskInput::SpecificView(schema_view_id.clone());
            assert!(schema_task(node.context.clone(), input).await.is_ok());

            let input = TaskInput::SpecificView(field_view_id);
            assert!(schema_task(node.context.clone(), input).await.is_ok());

            // The new schema should be available on storage provider.
            let schema = node
                .context
                .schema_provider
                .get(&SchemaId::Application(
                    SchemaName::new("schema_name").unwrap(),
                    schema_view_id.clone(),
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
