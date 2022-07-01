// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::document::{DocumentId, DocumentViewId};
use p2panda_rs::schema::system::SystemSchemaError;
use p2panda_rs::schema::{SchemaError, SchemaIdError};

/// `SQLStorage` errors.
#[derive(thiserror::Error, Debug)]
pub enum SqlStorageError {
    #[error("SQL query failed: {0}")]
    Transaction(String),

    #[error("Deletion of row from table {0} did not show any effect")]
    Deletion(String),
}

/// `DocumentStore` errors.
#[derive(thiserror::Error, Debug)]
pub enum DocumentStorageError {
    /// Catch all error which implementers can use for passing their own errors up the chain.
    #[allow(dead_code)]
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

/// `SchemaStore` errors.
#[derive(thiserror::Error, Debug)]
pub enum SchemaStoreError {
    /// Catch all error which implementers can use for passing their own errors up the chain.
    #[error("Error occured in DocumentStore: {0}")]
    #[allow(dead_code)]
    Custom(String),

    /// Error returned when no document view existed for the required schema field definition
    #[error(
        "No document view found for schema field definition with id: {0} which is required by schema definition {1}"
    )]
    MissingSchemaFieldDefinition(DocumentViewId, DocumentViewId),

    /// Error returned from converting p2panda-rs `DocumentView` into `SchemaView.
    #[error(transparent)]
    SystemSchemaError(#[from] SystemSchemaError),

    /// Error returned from p2panda-rs `Schema` methods.
    #[error(transparent)]
    SchemaError(#[from] SchemaError),

    /// Error returned from p2panda-rs `SchemaId` methods.
    #[error(transparent)]
    SchemaIdError(#[from] SchemaIdError),

    /// Error returned from `DocumentStore` methods.
    #[error(transparent)]
    DocumentStorageError(#[from] DocumentStorageError),
}
