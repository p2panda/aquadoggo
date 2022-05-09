// SPDX-License-Identifier: AGPL-3.0-or-later

/// `OperationStore` errors.
#[derive(thiserror::Error, Debug)]
pub enum OperationStorageError {
    /// Catch all error which implementers can use for passing their own errors up the chain.
    #[error("Error occured in OperationStore: {0}")]
    Custom(String),
}
