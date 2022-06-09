// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::context::Context;
use crate::db::errors::SchemaStoreError;
use crate::db::traits::SchemaStore;
use crate::materializer::worker::{TaskError, TaskResult};
use crate::materializer::TaskInput;

pub async fn schema_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    match (&input.document_id, &input.document_view_id) {
        (None, Some(document_view_id)) => {
            match context.store.get_schema_by_id(document_view_id).await {
                Ok(schema) => {
                    match schema {
                        Some(_schema) => {
                            // Get the schema into the schema service somehow //

                            Ok(None)
                        }
                        None => Ok(None),
                    }
                }
                Err(e) => match e {
                    SchemaStoreError::MissingSchemaFieldDefinition(_, _) => {
                        // If this was a schema definition and it's dependencies aren't met, there's something wrong with our task logic
                        Err(TaskError::Critical)
                    }
                    // If there was a fatal storage error we should crash
                    SchemaStoreError::DocumentStorageError(_) => Err(TaskError::Critical),
                    // All other errors just mean this wasn't a schema definition
                    _ => Ok(None),
                },
            }
        }
        (_, _) => Err(TaskError::Critical),
    }
}
