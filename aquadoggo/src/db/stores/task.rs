// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use p2panda_rs::document::{DocumentId, DocumentViewId};
use sqlx::{query, query_as};

use crate::db::errors::SqlStoreError;
use crate::db::models::TaskRow;
use crate::db::SqlStore;
use crate::materializer::{Task, TaskInput};

/// Methods to interact with the `tasks` table in the database.
impl SqlStore {
    /// Inserts a "pending" task into the database.
    pub async fn insert_task(&self, task: &Task<TaskInput>) -> Result<(), SqlStoreError> {
        // Convert task input to correct database types
        let task_input = task.input();
        let document_id = task_input.document_id.as_ref().map(|id| id.as_str());
        let document_view_id = task_input
            .document_view_id
            .as_ref()
            .map(|view_id| view_id.to_string());

        // Insert task into database
        query(
            "
            INSERT INTO
                tasks (
                    name,
                    document_id,
                    document_view_id
                )
            VALUES
                ($1, $2, $3)
            ON CONFLICT DO NOTHING
            ",
        )
        .bind(task.worker_name())
        .bind(document_id)
        .bind(document_view_id)
        .execute(&self.pool)
        .await
        .map_err(|err| SqlStoreError::Transaction(err.to_string()))?;

        Ok(())
    }

    /// Removes a "pending" task from the database.
    pub async fn remove_task(&self, task: &Task<TaskInput>) -> Result<(), SqlStoreError> {
        // Convert task input to correct database types
        let task_input = task.input();
        let document_id = task_input.document_id.as_ref().map(|id| id.as_str());
        let document_view_id = task_input
            .document_view_id
            .as_ref()
            .map(|view_id| view_id.to_string());

        // Remove task from database
        let result = query(
            "
            DELETE FROM
                tasks
            WHERE
                name = $1
                -- Use `COALESCE` to compare possible null values in a way
                -- that is compatible between SQLite and PostgreSQL.
                AND COALESCE(document_id, '0') = COALESCE($2, '0')
                AND COALESCE(document_view_id, '0') = COALESCE($3, '0')
            ",
        )
        .bind(task.worker_name())
        .bind(document_id)
        .bind(document_view_id)
        .execute(&self.pool)
        .await
        .map_err(|err| SqlStoreError::Transaction(err.to_string()))?;

        if result.rows_affected() != 1 {
            Err(SqlStoreError::Deletion("tasks".into()))
        } else {
            Ok(())
        }
    }

    /// Returns "pending" tasks of the materialization service worker.
    pub async fn get_tasks(&self) -> Result<Vec<Task<TaskInput>>, SqlStoreError> {
        let task_rows = query_as::<_, TaskRow>(
            "
            SELECT
                name,
                document_id,
                document_view_id
            FROM
                tasks
            ",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|err| SqlStoreError::Transaction(err.to_string()))?;

        // Convert database rows into correct p2panda types
        let mut tasks: Vec<Task<TaskInput>> = Vec::new();
        for task in task_rows {
            let document_id: Option<DocumentId> = task.document_id.map(|id| {
                id.parse()
                    .unwrap_or_else(|_| panic!("Invalid document id stored in database {}", id))
            });

            let document_view_id: Option<DocumentViewId> = task.document_view_id.map(|view_id| {
                view_id.parse().unwrap_or_else(|_| {
                    panic!("Invalid document view id stored in database: {}", view_id)
                })
            });

            tasks.push(Task::new(
                &task.name,
                TaskInput::new(document_id, document_view_id),
            ));
        }

        Ok(tasks)
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::document::{DocumentId, DocumentViewId};
    use p2panda_rs::test_utils::fixtures::{document_id, document_view_id};
    use rstest::rstest;

    use crate::test_utils::{test_db, TestDatabase, TestDatabaseRunner};
    use crate::materializer::{Task, TaskInput};

    #[rstest]
    fn insert_get_remove_tasks(
        document_view_id: DocumentViewId,
        #[from(test_db)] runner: TestDatabaseRunner,
    ) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            // Prepare test data
            let task = Task::new("reduce", TaskInput::new(None, Some(document_view_id)));

            // Insert task
            let result = db.store.insert_task(&task).await;
            assert!(result.is_ok(), "{:?}", result);

            // Check if task exists in database
            let result = db.store.get_tasks().await;
            assert_eq!(result.unwrap(), vec![task.clone()]);

            // Remove task
            let result = db.store.remove_task(&task).await;
            assert!(result.is_ok(), "{:?}", result);

            // Check if all tasks got removed
            let result = db.store.get_tasks().await;
            assert_eq!(result.unwrap(), vec![]);
        });
    }

    #[rstest]
    fn avoid_duplicates(document_id: DocumentId, #[from(test_db)] runner: TestDatabaseRunner) {
        runner.with_db_teardown(|db: TestDatabase| async move {
            // Prepare test data
            let task = Task::new("reduce", TaskInput::new(Some(document_id), None));

            // Insert task
            let result = db.store.insert_task(&task).await;
            assert!(result.is_ok(), "{:?}", result);

            // Insert the same thing again, it should silently fail
            let result = db.store.insert_task(&task).await;
            assert!(result.is_ok(), "{:?}", result);

            // Check for duplicates
            let result = db.store.get_tasks().await;
            assert_eq!(result.unwrap().len(), 1);
        });
    }
}
