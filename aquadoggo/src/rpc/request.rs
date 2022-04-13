// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::DocumentId;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::{AsEntryArgsRequest, AsPublishEntryRequest};
use serde::Deserialize;

use p2panda_rs::entry::EntrySigned;
use p2panda_rs::identity::Author;
use p2panda_rs::operation::OperationEncoded;

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

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryEntriesRequest {
    pub schema: SchemaId,
}
