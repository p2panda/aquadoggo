// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use p2panda_rs::document::{DocumentId, DocumentViewId};

use crate::db::models::TaskRow;
use crate::db::provider::SqlStorage;
use crate::materializer::{Task, TaskInput};

/// Methods to interact with the `tasks` table in the database.
impl SqlStorage {
    /// Inserts a "pending" task into the database.
    pub async fn insert_task(&self, task: &Task<TaskInput>) -> Result<()> {
        Ok(())
    }

    /// Removes a "pending" task from the database.
    pub async fn remove_task(&self, task: &Task<TaskInput>) -> Result<()> {
        Ok(())
    }

    /// Returns "pending" tasks of the materialization service worker.
    pub async fn get_tasks(&self) -> Result<Vec<Task<TaskInput>>> {
        Ok(vec![])
    }
}
