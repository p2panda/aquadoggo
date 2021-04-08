use serde::Serialize;

use p2panda_rs::atomic::{Hash, LogId, SeqNum};

/// Response body of `panda_getEntryArguments`.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct EntryArgsResponse {
    pub entry_hash_backlink: Option<Hash>,
    pub entry_hash_skiplink: Option<Hash>,
    pub last_seq_num: Option<SeqNum>,
    pub log_id: LogId,
}

/// Response body of `panda_publishEntry`.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PublishEntryResponse {
    pub entry_hash_backlink: Option<Hash>,
    pub entry_hash_skiplink: Option<Hash>,
    pub last_seq_num: Option<SeqNum>,
    pub log_id: LogId,
}
