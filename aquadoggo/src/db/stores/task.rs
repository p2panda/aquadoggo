// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use sqlx::{query, query_as};

use crate::db::errors::SqlStorageError;
use crate::db::models::TaskRow;
use crate::db::provider::SqlStorage;
use crate::materializer::{Task, TaskInput};

/// Methods to interact with the `tasks` table in the database.
impl SqlStorage {
    /// Inserts a "pending" task into the database.
    pub async fn insert_task(&self, task: &Task<TaskInput>) -> Result<(), SqlStorageError> {
        // Convert task input to correct database types
        let task_input = task.input();
        let document_id = task_input.document_id.as_ref().map(|id| id.to_string());
        let document_view_id = task_input
            .document_view_id
            .as_ref()
            .map(|view_id| view_id.to_string());

        // Insert task into database
        let result = query(
            "
            INSERT INTO
                tasks (
                    name,
                    document_id,
                    document_view_id
                )
            VALUES
                ($1, $2, $3)
            ",
        )
        .bind(task.worker_name())
        .bind(document_id)
        .bind(document_view_id)
        .execute(&self.pool)
        .await
        .map_err(|err| SqlStorageError::TransactionFailed(err.to_string()))?;

        if result.rows_affected() != 1 {
            Err(SqlStorageError::InsertionFailed("tasks".into()))
        } else {
            Ok(())
        }
    }

    /// Removes a "pending" task from the database.
    pub async fn remove_task(&self, task: &Task<TaskInput>) -> Result<(), SqlStorageError> {
        // Convert task input to correct database types
        let task_input = task.input();
        let document_id = task_input.document_id.as_ref().map(|id| id.to_string());
        let document_view_id = task_input
            .document_view_id
            .as_ref()
            .map(|view_id| view_id.to_string());

        // Remove task from database
        query(
            "
            DELETE
            FROM
                tasks
            WHERE
                name = $1
                    AND document_id = $2
                    AND document_view_id = $3
            ",
        )
        .bind(task.worker_name())
        .bind(document_id)
        .bind(document_view_id)
        .execute(&self.pool)
        .await
        .map_err(|err| SqlStorageError::TransactionFailed(err.to_string()))?;

        Ok(())
    }

    /// Returns "pending" tasks of the materialization service worker.
    pub async fn get_tasks(&self) -> Result<Vec<Task<TaskInput>>, SqlStorageError> {
        let task_rows = query_as::<_, TaskRow>(
            "
            SELECT
                name
                document_id,
                document_view_id
            FROM
                tasks
            ",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|err| SqlStorageError::TransactionFailed(err.to_string()))?;

        // Convert database rows into correct p2panda types
        let mut tasks: Vec<Task<TaskInput>> = Vec::new();
        for task in task_rows {
            let document_id: Option<DocumentId> = task
                .document_id
                .map(|id| id.parse().expect("Invalid document id stored in database"));

            let document_view_id: Option<DocumentViewId> = task.document_view_id.map(|view_id| {
                view_id
                    .parse()
                    .expect("Invalid document view id stored in database")
            });

            tasks.push(Task::new(
                &task.name,
                TaskInput::new(document_id, document_view_id),
            ));
        }

        Ok(tasks)
    }
}
