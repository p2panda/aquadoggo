// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::{DocumentId, DocumentViewFields, DocumentViewId};
use p2panda_rs::identity::PublicKey;
use p2panda_rs::schema::SchemaId;

#[derive(Debug, Clone, PartialEq)]
pub struct StorageDocument {
    /// The id for this document.
    pub(crate) id: DocumentId,

    /// The key-value mapping of this documents current view.
    pub(crate) fields: Option<DocumentViewFields>,

    /// The id of the schema this document follows.
    pub(crate) schema_id: SchemaId,

    /// The id of the current view of this document.
    pub(crate) view_id: DocumentViewId,

    /// The public key of the author who created this document.
    pub(crate) author: PublicKey,

    /// Flag indicating if document was deleted.
    pub(crate) deleted: bool,
}

impl AsDocument for StorageDocument {
    /// Get the document id.
    fn id(&self) -> &DocumentId {
        &self.id
    }

    /// Get the document view id.
    fn view_id(&self) -> &DocumentViewId {
        &self.view_id
    }

    /// Get the document author's public key.
    fn author(&self) -> &PublicKey {
        &self.author
    }

    /// Get the document schema.
    fn schema_id(&self) -> &SchemaId {
        &self.schema_id
    }

    /// The key-value mapping of this documents current view.
    fn fields(&self) -> Option<&DocumentViewFields> {
        self.fields.as_ref()
    }

    /// Returns true if this document has applied an UPDATE operation.
    fn is_edited(&self) -> bool {
        match self.fields() {
            Some(fields) => fields
                .iter()
                .find(|(_, document_view_value)| {
                    &DocumentId::new(document_view_value.id()) != self.id()
                })
                .is_none(),
            None => true,
        }
    }

    /// Returns true if this document has processed a DELETE operation.
    fn is_deleted(&self) -> bool {
        self.deleted
    }
}
