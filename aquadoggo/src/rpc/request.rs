use serde::Deserialize;

use p2panda_rs::atomic::{Author, EntrySigned, Hash, MessageEncoded};

/// Request body of `panda_getEntryArguments`.
#[derive(Deserialize, Debug)]
pub struct EntryArgsRequest {
    pub author: Author,
    pub schema: Hash,
}

/// Request body of `panda_publishEntry`.
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PublishEntryRequest {
    pub entry_encoded: EntrySigned,
    pub message_encoded: MessageEncoded,
}
