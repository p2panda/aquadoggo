// SPDX-License-Identifier: AGPL-3.0-or-later

use sqlx::FromRow;

/// Tracks the assigment of an author's logs to documents and records their schema.
///
/// This serves as an indexing layer on top of the lower-level bamboo entries. The node updates
/// this data according to what it sees in the newly incoming entries.
///
/// We store the u64 integer values of `log_id` as a string here since SQLite doesn't support
/// storing unsigned 64 bit integers.
#[derive(FromRow, Debug, Clone)]
pub struct LogRow {
    /// Public key of the author.
    pub author: String,

    /// Log id used for this document.
    pub log_id: String,

    /// Hash that identifies the document this log is for.
    pub document: String,

    /// SchemaId which identifies the schema for operations in this log.
    pub schema: String,
}
