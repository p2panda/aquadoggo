// SPDX-License-Identifier: AGPL-3.0-or-later

use serde::Serialize;

use crate::db::models::{Entry, EntryRow};
use p2panda_rs::{hash::Hash, storage_provider::responses::AsEntryArgsResponse};

/// Response body of `panda_getEntryArguments`.
///
/// `seq_num` and `log_id` are returned as strings to be able to represent large integers in JSON.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct EntryArgsResponse {
    pub entry_hash_backlink: Option<Hash>,
    pub entry_hash_skiplink: Option<Hash>,
    pub seq_num: String,
    pub log_id: String,
}

impl AsEntryArgsResponse for EntryArgsResponse {
    fn new(
        entry_hash_backlink: Option<Hash>,
        entry_hash_skiplink: Option<Hash>,
        seq_num: p2panda_rs::entry::SeqNum,
        log_id: p2panda_rs::entry::LogId,
    ) -> Self {
        EntryArgsResponse {
            entry_hash_backlink,
            entry_hash_skiplink,
            seq_num: seq_num.as_u64().to_string(),
            log_id: log_id.as_u64().to_string(),
        }
    }
}

/// Response body of `panda_publishEntry`.
///
/// `seq_num` and `log_id` are returned as strings to be able to represent large integers in JSON.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PublishEntryResponse {
    pub entry_hash_backlink: Option<Hash>,
    pub entry_hash_skiplink: Option<Hash>,
    pub seq_num: String,
    pub log_id: String,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryEntriesResponse {
    pub entries: Vec<Entry>,
}
