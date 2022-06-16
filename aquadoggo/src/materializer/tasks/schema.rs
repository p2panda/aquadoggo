// SPDX-License-Identifier: AGPL-3.0-or-later

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
    let input_view_id = match (input.document_id, input.document_view_id) {
        (None, Some(view_id)) => Ok(view_id),
        // The task input must contain only a view id.
        (_, _) => Err(TaskError::Critical),
    }?;

    // Determine the schema of the updated view id.
    let sample_operation = context
        .store
        .get_operation_by_id(input_view_id.graph_tips().first().unwrap())
        .await
        .map_err(|_err| TaskError::Critical)?
        .unwrap();
    let schema = sample_operation.operation().schema();

    let updated_schema_definitions: Vec<DocumentViewId> = match schema {
        // This task is about an updated schema definition document so we only handle that.
        SchemaId::SchemaDefinition(_) => Ok(vec![input_view_id.clone()]),

        // This task is about an updated schema field definition document that may be used by
        // multiple schema definition documents so we must handle all of those.
        SchemaId::SchemaFieldDefinition(_) => {
            get_related_schema_definitions(&input_view_id, &context).await
        }
        _ => Err(TaskError::Critical),
    }?;

    if updated_schema_definitions.is_empty() {
        return Err(TaskError::Failure);
    }

    for view_id in updated_schema_definitions.iter() {
        match context
            .store
            .get_schema_by_id(view_id)
            .await
            .map_err(|_err| TaskError::Critical)?
        {
            Some(schema) => {
                context.schemas.update(schema);
            }
            None => (),
        };
    }

    Ok(None)
}

/// Retrieve schema definitions that use the targetted schema field definition as one of their fields.
async fn get_related_schema_definitions(
    target_field_definition: &DocumentViewId,
    context: &Context,
) -> Result<Vec<DocumentViewId>, TaskError> {
    // Retrieve all schema definition documents from the store.
    let schema_definitions = context
        .store
        .get_documents_by_schema(&SchemaId::SchemaDefinition(1))
        .await
        .map_err(|_err| TaskError::Critical)
        .unwrap();

    // Collect all schema definitions that use the targetted field definition
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
            Err(TaskError::Critical)?
        }
    }

    Ok(related_schema_definitions)
}
