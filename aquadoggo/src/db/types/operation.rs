// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::identity::PublicKey;
use p2panda_rs::operation::traits::{AsOperation, WithPublicKey};
use p2panda_rs::operation::{OperationAction, OperationFields, OperationId, OperationVersion};
use p2panda_rs::schema::SchemaId;
use p2panda_rs::WithId;

#[derive(Debug, Clone, PartialEq)]
pub struct StorageOperation {
    /// The id of the document this operation is part of.
    pub(crate) document_id: DocumentId,

    /// Identifier of the operation.
    pub(crate) id: OperationId,

    /// Version of this operation.
    pub(crate) version: OperationVersion,

    /// Action of this operation.
    pub(crate) action: OperationAction,

    /// Schema instance of this operation.
    pub(crate) schema_id: SchemaId,

    /// Previous operations field.
    pub(crate) previous: Option<DocumentViewId>,

    /// Operation fields.
    pub(crate) fields: Option<OperationFields>,

    /// The public key of the key pair used to publish this operation.
    pub(crate) public_key: PublicKey,

    /// Index for the position of this operation once topological sorting of the operation graph
    /// has been performed.
    ///
    /// Is `None` when the operation has not been materialized into it's document yet.
    pub(crate) sorted_index: Option<i32>,
}

impl WithPublicKey for StorageOperation {
    /// Returns the public key of the author of this operation.
    fn public_key(&self) -> &PublicKey {
        &self.public_key
    }
}

impl WithId<OperationId> for StorageOperation {
    /// Returns the identifier for this operation.
    fn id(&self) -> &OperationId {
        &self.id
    }
}

impl WithId<DocumentId> for StorageOperation {
    /// Returns the identifier for this operation.
    fn id(&self) -> &DocumentId {
        &self.document_id
    }
}

impl AsOperation for StorageOperation {
    /// Returns action type of operation.
    fn action(&self) -> OperationAction {
        self.action.to_owned()
    }

    /// Returns schema id of operation.
    fn schema_id(&self) -> SchemaId {
        self.schema_id.to_owned()
    }

    /// Returns version of operation.
    fn version(&self) -> OperationVersion {
        self.version.to_owned()
    }

    /// Returns application data fields of operation.
    fn fields(&self) -> Option<OperationFields> {
        self.fields.clone()
    }

    /// Returns vector of this operation's previous operation ids.
    fn previous(&self) -> Option<DocumentViewId> {
        self.previous.clone()
    }
}
