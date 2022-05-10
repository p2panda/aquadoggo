// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::operation::OperationId;

/// `OperationStore` errors.
#[derive(thiserror::Error, Debug)]
pub enum OperationStorageError {
    /// Catch all error which implementers can use for passing their own errors up the chain.
    #[error("Error occured in OperationStore: {0}")]
    Custom(String),

    /// Error which originates in `insert_operation()` when the insertion fails.
    #[error("Error occured when inserting an operation with id {0:?} into storage")]
    InsertionError(OperationId),
}
