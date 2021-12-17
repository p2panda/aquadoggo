// SPDX-License-Identifier: AGPL-3.0-or-later

use serde::Deserialize;

use p2panda_rs::entry::EntrySigned;
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::Author;
use p2panda_rs::operation::OperationEncoded;

/// Request body of `panda_getEntryArguments`.
#[derive(Deserialize, Debug)]
pub struct EntryArgsRequest {
    pub author: Author,
    pub document: Option<Hash>,
}

/// Request body of `panda_publishEntry`.
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PublishEntryRequest {
    pub entry_encoded: EntrySigned,
    pub operation_encoded: OperationEncoded,
}
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryEntriesRequest {
    pub schema: Hash,
}
