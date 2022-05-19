// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::{DocumentId, DocumentViewId};

/// `DocumentStore` errors.
#[derive(thiserror::Error, Debug)]
pub enum DocumentStorageError {
    /// Catch all error which implementers can use for passing their own errors up the chain.
    #[error("Error occured in DocumentStore: {0}")]
    Custom(String),

    /// A fatal error occured when performing a storage query.
    #[error("A fatal error occured in DocumentStore: {0}")]
    FatalStorageError(String),

    /// Error which originates in `insert_document_view()` when the insertion fails.
    #[error("Error occured when inserting a document view with id {0:?} into storage")]
    DocumentViewInsertionError(DocumentViewId),

    /// Error which originates in `insert_document()` when the insertion fails.
    #[error("Error occured when inserting a document with id {0:?} into storage")]
    DocumentInsertionError(DocumentId),
}
