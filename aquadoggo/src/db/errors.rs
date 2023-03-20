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

/// `Query` API errors.
#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Can't apply ordering on unknown field '{0}'")]
    OrderFieldUnknown(String),

    #[error("Can't apply filter on unknown field '{0}'")]
    FilterFieldUnknown(String),

    #[error("Filter type {0} for field '{1}' is not matching schema type {2}")]
    FilterInvalidType(String, String, String),

    #[error("Can't apply set filter as field '{0}' is of type boolean")]
    FilterInvalidSet(String),

    #[error("Can't apply interval filter as field '{0}' is not of type string, float or integer")]
    FilterInvalidInterval(String),

    #[error("Can't apply search filter as field '{0}' is not of type string")]
    FilterInvalidSearch(String),
}
