// SPDX-License-Identifier: AGPL-3.0-or-later

use sqlx::FromRow;

/// Representation of a row from the logs table as stored in the database. This is required
/// when coercing the returned results from a query with the `sqlx` library.
///
/// We store the u64 integer values of `log_id` as a string here since SQLite doesn't support
/// storing unsigned 64 bit integers.
#[derive(FromRow, Debug, Clone)]
pub struct LogRow {
    /// Public key of the author.
    pub public_key: String,

    /// Log id used for this document.
    pub log_id: String,

    /// Hash that identifies the document this log is for.
    pub document: String,

    /// SchemaId which identifies the schema for operations in this log.
    pub schema: String,
}

#[derive(FromRow, Debug, Clone)]
pub struct LogHeightRow {
    /// PublicKey of this entry.
    pub public_key: String,

    /// Used log for this entry.
    pub log_id: String,

    /// Sequence number of this entry.
    pub seq_num: String,
}
