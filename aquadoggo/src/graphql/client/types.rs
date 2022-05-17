// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::SimpleObject;
use p2panda_rs::document::DocumentId;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::{AsEntryArgsRequest, AsPublishEntryRequest};
use p2panda_rs::storage_provider::ValidationError;
use p2panda_rs::Validate;
use serde::{Deserialize, Serialize};

use p2panda_rs::entry::{decode_entry, EntrySigned, LogId, SeqNum};
use p2panda_rs::hash::Hash;
use p2panda_rs::identity::Author;
use p2panda_rs::operation::OperationEncoded;
use p2panda_rs::storage_provider::traits::{AsEntryArgsResponse, AsPublishEntryResponse};

use crate::db::models::entry::EntryRow;

/// Request body of `panda_getEntryArguments`.
#[derive(Deserialize, Debug)]
pub struct EntryArgsRequest {
    pub author: Author,
    pub document: Option<DocumentId>,
}

impl AsEntryArgsRequest for EntryArgsRequest {
    fn author(&self) -> &Author {
        &self.author
    }

    fn document_id(&self) -> &Option<DocumentId> {
        &self.document
    }
}

impl Validate for EntryArgsRequest {
    type Error = ValidationError;

    fn validate(&self) -> Result<(), Self::Error> {
        // Validate `author` request parameter
        self.author().validate()?;

        // Validate `document` request parameter when it is set
        match self.document_id() {
            None => (),
            Some(doc) => {
                doc.validate()?;
            }
        };
        Ok(())
    }
}

/// Response body of `panda_getEntryArguments`.
///
/// `seq_num` and `log_id` are returned as strings to be able to represent large integers in JSON.
#[derive(Serialize, Debug, SimpleObject)]
#[serde(rename_all = "camelCase")]
pub struct EntryArgsResponse {
    pub backlink: Option<String>,
    pub skiplink: Option<String>,
    pub seq_num: String,
    pub log_id: String,
}

impl AsEntryArgsResponse for EntryArgsResponse {
    fn new(backlink: Option<Hash>, skiplink: Option<Hash>, seq_num: SeqNum, log_id: LogId) -> Self {
        EntryArgsResponse {
            backlink: backlink.map(|hash| hash.as_str().to_string()),
            skiplink: skiplink.map(|hash| hash.as_str().to_string()),
            seq_num: seq_num.as_u64().to_string(),
            log_id: log_id.as_u64().to_string(),
        }
    }
}

/// Request body of `panda_publishEntry`.
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PublishEntryRequest {
    pub entry_encoded: EntrySigned,
    pub operation_encoded: OperationEncoded,
}

impl AsPublishEntryRequest for PublishEntryRequest {
    fn entry_signed(&self) -> &EntrySigned {
        &self.entry_encoded
    }

    fn operation_encoded(&self) -> &OperationEncoded {
        &self.operation_encoded
    }
}

impl Validate for PublishEntryRequest {
    type Error = ValidationError;

    fn validate(&self) -> Result<(), Self::Error> {
        self.entry_signed().validate()?;
        self.operation_encoded().validate()?;
        decode_entry(self.entry_signed(), Some(self.operation_encoded()))?;
        Ok(())
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryEntriesRequest {
    pub schema: SchemaId,
}

/// Response body of `panda_publishEntry`.
///
/// `seq_num` and `log_id` are returned as strings to be able to represent large integers in JSON.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PublishEntryResponse {
    pub entry_hash_backlink: Option<Hash>,
    pub entry_hash_skiplink: Option<Hash>,
    pub seq_num: SeqNum,
    pub log_id: LogId,
}

impl AsPublishEntryResponse for PublishEntryResponse {
    fn new(
        entry_hash_backlink: Option<Hash>,
        entry_hash_skiplink: Option<Hash>,
        seq_num: SeqNum,
        log_id: LogId,
    ) -> Self {
        PublishEntryResponse {
            entry_hash_backlink,
            entry_hash_skiplink,
            seq_num,
            log_id,
        }
    }
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryEntriesResponse {
    pub entries: Vec<EntryRow>,
}
