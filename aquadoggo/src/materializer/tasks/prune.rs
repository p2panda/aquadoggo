// SPDX-License-Identifier: AGPL-3.0-or-later

use log::debug;
use p2panda_rs::Human;

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
            debug!("Prune document views for document: {}", id.display());
            let effected_child_relations = context
                .store
                .prune_document_views(&id)
                .await
                .map_err(|err| TaskError::Critical(err.to_string()))?;

            // Compose next tasks.
            let next_tasks: Vec<Task<TaskInput>> = effected_child_relations
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
        _ => Err(TaskError::Critical("Invalid task input".into())),
    }
}
