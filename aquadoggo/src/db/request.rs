// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::DocumentId;
use p2panda_rs::storage_provider::traits::{AsEntryArgsRequest, AsPublishEntryRequest};
use p2panda_rs::storage_provider::ValidationError;
use p2panda_rs::Validate;
use serde::Deserialize;

use p2panda_rs::entry::{decode_entry, EntrySigned};
use p2panda_rs::identity::Author;
use p2panda_rs::operation::OperationEncoded;

/// Struct used to validate params and query database to retreive next entry arguments.
#[derive(Deserialize, Debug)]
pub struct EntryArgsRequest {
    /// The entry author.
    pub public_key: Author,

    /// The entry document id.
    pub document_id: Option<DocumentId>,
}

impl AsEntryArgsRequest for EntryArgsRequest {
    fn author(&self) -> &Author {
        &self.public_key
    }

    fn document_id(&self) -> &Option<DocumentId> {
        &self.document_id
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

/// Struct used to validate params and publish new entry in database.
#[derive(Deserialize, Debug)]
pub struct PublishEntryRequest {
    /// The encoded entry
    pub entry: EntrySigned,

    /// The encoded operation
    pub operation: OperationEncoded,
}

impl AsPublishEntryRequest for PublishEntryRequest {
    fn entry_signed(&self) -> &EntrySigned {
        &self.entry
    }

    fn operation_encoded(&self) -> &OperationEncoded {
        &self.operation
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
