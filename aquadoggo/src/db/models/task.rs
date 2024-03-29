// SPDX-License-Identifier: AGPL-3.0-or-later

use sqlx::FromRow;

/// Representation of a row from the `tasks` table as stored in the database.
///
/// This table holds all "pending" tasks of the materialization service worker.
#[derive(FromRow, Debug, Clone, PartialEq, Eq)]
pub struct TaskRow {
    /// Name of the task worker.
    pub name: String,

    /// `DocumentId` of the task input.
    pub document_id: Option<String>,

    /// `DocumentViewId` of the task input.
    pub document_view_id: Option<String>,
}
