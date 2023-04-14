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
}

/// `SchemaStore` errors.
#[derive(Error, Debug)]
pub enum SchemaStoreError {
    /// Catch all error which implementers can use for passing their own errors up the chain.
    #[error("Error occured in DocumentStore: {0}")]
    #[allow(dead_code)]
    Custom(String),

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

    /// Error returned from `OperationStore` methods.
    #[error(transparent)]
    OperationStorageError(#[from] OperationStorageError),
}
