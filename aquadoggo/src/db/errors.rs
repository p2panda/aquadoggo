// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::schema::error::{SchemaError, SchemaIdError};
use p2panda_rs::schema::system::SystemSchemaError;
use p2panda_rs::storage_provider::error::{DocumentStorageError, OperationStorageError};
use thiserror::Error;

/// `SQLStorage` errors.
#[derive(Error, Debug)]
pub enum SqlStoreError {
    #[error("SQL query failed: {0}")]
    Transaction(String),

    #[error("Deletion of row from table {0} did not show any effect")]
    Deletion(String),

    /// Error returned from BlobStore.
    #[error(transparent)]
    BlobStoreError(#[from] BlobStoreError),

    /// Error returned from `DocumentStore` methods.
    #[error(transparent)]
    DocumentStorage(#[from] DocumentStorageError),
}

/// `SchemaStore` errors.
#[derive(Error, Debug)]
pub enum SchemaStoreError {
    /// Error returned from converting p2panda-rs `DocumentView` into `SchemaView.
    #[error(transparent)]
    SystemSchema(#[from] SystemSchemaError),

    /// Error returned from p2panda-rs `Schema` methods.
    #[error(transparent)]
    Schema(#[from] SchemaError),

    /// Error returned from p2panda-rs `SchemaId` methods.
    #[error(transparent)]
    SchemaId(#[from] SchemaIdError),

    /// Error returned from `DocumentStore` methods.
    #[error(transparent)]
    DocumentStorage(#[from] DocumentStorageError),

    /// Error returned from `OperationStore` methods.
    #[error(transparent)]
    OperationStorage(#[from] OperationStorageError),
}

#[derive(Error, Debug)]
pub enum BlobStoreError {
    /// Error when no "pieces" field found on blob document.
    #[error("Missing \"pieces\" field on blob document")]
    NotBlobDocument,

    /// Error when no pieces found for existing blob document.
    #[error("No pieces found for the requested blob")]
    NoBlobPiecesFound,

    /// Error when some pieces not found for existing blob document.
    #[error("Some pieces missing for the requested blob")]
    MissingPieces,

    /// Error when combined pieces length and claimed blob length don't match.
    #[error("The combined pieces length and claimed blob length don't match")]
    IncorrectLength,

    /// Error returned from `DocumentStore` methods.
    #[error(transparent)]
    DocumentStorageError(#[from] DocumentStorageError),
}
