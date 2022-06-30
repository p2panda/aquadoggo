// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::DocumentId;
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::{AsEntryArgsRequest, AsPublishEntryRequest};
use p2panda_rs::storage_provider::ValidationError;
use p2panda_rs::Validate;
use serde::Deserialize;

use p2panda_rs::entry::{decode_entry, EntrySigned};
use p2panda_rs::identity::Author;
use p2panda_rs::operation::OperationEncoded;

/// Request body of `nextEntryArgs`.
#[derive(Deserialize, Debug)]
pub struct EntryArgsRequest {
    /// The entry author.
    pub author: Author,

    /// The entry document id.
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

/// Request body of `publishEntry`.
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PublishEntryRequest {
    /// The encoded entry
    pub entry_encoded: EntrySigned,

    /// The encoded operation
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
