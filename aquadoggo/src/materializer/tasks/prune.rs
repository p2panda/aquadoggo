// SPDX-License-Identifier: AGPL-3.0-or-later

use log::debug;
use p2panda_rs::storage_provider::traits::OperationStore;

use crate::context::Context;
use crate::materializer::worker::{TaskError, TaskResult};
use crate::materializer::{Task, TaskInput};

pub async fn prune_task(context: Context, input: TaskInput) -> TaskResult<TaskInput> {
    debug!("Working on {}", input);

    match input {
        TaskInput::DocumentId(id) => {
            // This task is concerned with a document which has been reduced and may now have
            // dangling views. We want to check for this and delete any views which are no longer
            // needed.
            let pruned_document_view_ids = context
                .store
                .prune_document_views(&id)
                .await
                .map_err(|err| TaskError::Critical(err.to_string()))?;

            debug!("Removed document views: {pruned_document_view_ids:#?}");
            let mut prune_next_documents = vec![];
            for document_view_id in pruned_document_view_ids {
                let document_id = context
                    .store
                    .get_document_id_by_operation_id(document_view_id.graph_tips().first().unwrap())
                    .await
                    .map_err(|err| TaskError::Critical(err.to_string()))?;
                if let Some(document_id) = document_id {
                    prune_next_documents.push(document_id);
                };
            }

            prune_next_documents.dedup();
            let next_tasks: Vec<Task<TaskInput>> = prune_next_documents
                .iter()
                .map(|document_id| {
                    debug!("Issue prune task for document: {document_id:#?}");
                    Task::new("prune", TaskInput::DocumentId(document_id.to_owned()))
                })
                .collect();

            if next_tasks.is_empty() {
                Ok(None)
            } else {
                Ok(Some(next_tasks))
            }
        }
        _ => return Err(TaskError::Critical("Invalid task input".into())),
    }
}
