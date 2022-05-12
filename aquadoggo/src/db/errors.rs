// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::{document::DocumentViewId, operation::OperationId};

/// `OperationStore` errors.
#[derive(thiserror::Error, Debug)]
pub enum OperationStorageError {
    /// Catch all error which implementers can use for passing their own errors up the chain.
    #[error("Error occured in OperationStore: {0}")]
    Custom(String),

    /// A fatal error occured when performing a storage query.
    #[error("A fatal error occured in OperationStore: {0}")]
    FatalStorageError(String),

    /// Error which originates in `insert_operation()` when the insertion fails.
    #[error("Error occured when inserting an operation with id {0:?} into storage")]
    InsertionError(OperationId),
}

/// `DocumentStore` errors.
#[derive(thiserror::Error, Debug)]
pub enum DocumentStorageError {
    /// Catch all error which implementers can use for passing their own errors up the chain.
    #[error("Error occured in DocumentStore: {0}")]
    Custom(String),

    /// A fatal error occured when performing a storage query.
    #[error("A fatal error occured in DocumentStore: {0}")]
    FatalStorageError(String),

    /// Error which originates in `insert_document()` when the insertion fails.
    #[error("Error occured when inserting a document view with id {0:?} into storage")]
    InsertionError(DocumentViewId),
}
