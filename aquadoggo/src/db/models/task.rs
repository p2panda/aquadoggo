// SPDX-License-Identifier: AGPL-3.0-or-later

use serde::Serialize;
use sqlx::FromRow;

/// Representation of a row from the `tasks` table as stored in the database.
///
/// This table holds all "pending" tasks of the materialization service worker.
#[derive(FromRow, Debug, Serialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskRow {
    /// Name of the task worker.
    pub name: String,

    /// Related `OperationId` of the pending task.
    pub operation_id: String,
}
