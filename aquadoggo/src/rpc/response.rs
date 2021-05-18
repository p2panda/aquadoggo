use serde::Serialize;

use crate::db::models::Entry;
use p2panda_rs::atomic::{Hash, LogId, SeqNum};

/// Response body of `panda_getEntryArguments`.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct EntryArgsResponse {
    pub entry_hash_backlink: Option<Hash>,
    pub entry_hash_skiplink: Option<Hash>,
    pub seq_num: SeqNum,
    pub log_id: LogId,
}

/// Response body of `panda_publishEntry`.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PublishEntryResponse {
    pub entry_hash_backlink: Option<Hash>,
    pub entry_hash_skiplink: Option<Hash>,
    pub seq_num: SeqNum,
    pub log_id: LogId,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryEntriesResponse {
    pub entries: Vec<Entry>,
}
