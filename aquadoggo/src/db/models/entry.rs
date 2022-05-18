// SPDX-License-Identifier: AGPL-3.0-or-later

use serde::Serialize;
use sqlx::FromRow;

/// Representation of a row from the entries table as stored in the database. This is required
/// when coercing the returned results from a query with the `sqlx` library.
///
/// We store the u64 integer values of `log_id` and `seq_num` as strings since SQLite doesn't
/// support storing unsigned 64 bit integers.
#[derive(FromRow, Debug, Serialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EntryRow {
    /// Public key of the author.
    pub author: String,

    /// Actual Bamboo entry data.
    pub entry_bytes: String,

    /// Hash of Bamboo entry data.
    pub entry_hash: String,

    /// Used log for this entry.
    pub log_id: String,

    /// Payload of entry, can be deleted.
    pub payload_bytes: Option<String>,

    /// Hash of payload data.
    pub payload_hash: String,

    /// Sequence number of this entry.
    pub seq_num: String,
}
