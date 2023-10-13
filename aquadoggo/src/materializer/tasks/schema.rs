// SPDX-License-Identifier: AGPL-3.0-or-later

use log::debug;

use crate::context::Context;
use crate::materializer::worker::{TaskError, TaskResult};
use crate::materializer::TaskInput;

/// A schema task assembles and stores schemas from their views.
///
/// Schema tasks are dispatched whenever a schema definition has all its immediate dependencies
/// available in the store. It collects all required views for the schema, instantiates it and
/// adds it to the schema provider.
pub async fn schema_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    debug!("Working on {}", input);

    let input_view_id = match input {
        TaskInput::DocumentViewId(view_id) => view_id,
        _ => return Err(TaskError::Critical("Invalid task input".into())),
    };

    // Get the schema from the store. This returns `None` if the schema is not present or incomplete.
    let schema = context
        .store
        .get_schema_by_id(&input_view_id)
        .await
        .map_err(|err| TaskError::Critical(err.to_string()))?;

    match schema {
        // Schema was assembled successfully and is now passed to schema provider.
        Some(schema) => {
            match context.schema_provider.update(schema.clone()).await {
                Ok(_) => (),
                Err(err) => debug!("Schema not supported: {}", err),
            };
        }
        // This schema was not ready to be assembled after all so it is ignored.
        None => {
            debug!("Not yet ready to build schema {}", input_view_id)
        }
    };

    Ok(None)
}

#[cfg(test)]
mod tests {
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::{OperationValue, PinnedRelationList};
    use p2panda_rs::schema::{FieldType, SchemaId, SchemaName};
    use p2panda_rs::test_utils::fixtures::key_pair;
    use rstest::rstest;

    use crate::materializer::tasks::dependency_task;
    use crate::materializer::{Task, TaskInput};
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

            // Run a schema task with the schema input
            let input = TaskInput::DocumentViewId(schema_view_id.clone());
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

    #[rstest]
    fn dependency_task_handles_fields_arriving_after_schema(key_pair: KeyPair) {
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

            // Run a dependency task with the schema input
            let input = TaskInput::DocumentViewId(schema_view_id.clone());
            let next_tasks = dependency_task(node.context.clone(), input)
                .await
                .unwrap()
                .unwrap();

            // It should return a single `schema` task
            assert_eq!(
                next_tasks,
                vec![Task::new(
                    "schema",
                    TaskInput::DocumentViewId(schema_view_id.clone())
                )]
            );

            // Run a dependency task with the schema field input
            let input = TaskInput::DocumentViewId(field_view_id.clone());
            let next_tasks = dependency_task(node.context.clone(), input)
                .await
                .unwrap()
                .unwrap();

            // It should return a single `dependency` task
            assert_eq!(
                next_tasks,
                vec![Task::new(
                    "dependency",
                    TaskInput::DocumentViewId(schema_view_id.clone()),
                )]
            );
        });
    }
}
