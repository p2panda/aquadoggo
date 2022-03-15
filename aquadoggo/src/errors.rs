// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::entry::{EntryError, EntrySignedError, LogIdError, SeqNumError};
use p2panda_rs::hash::HashError;
use p2panda_rs::identity::AuthorError;
use p2panda_rs::operation::{OperationEncodedError, OperationError};

/// A specialized result type for the storage provider errors.
pub type StorageProviderResult<T> =
    anyhow::Result<T, p2panda_rs::storage_provider::errors::StorageProviderError>;

// TODO: This isn't used yet.
// need to figure out the best way to handle errors in the storage_provider traits
#[derive(thiserror::Error, Debug)]
pub enum DatabaseErrors {
    /// Error returned from the database.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Represents all the ways a method can fail within the node.
#[derive(thiserror::Error, Debug)]
pub enum ValidationErrors {
    /// Error returned from validating p2panda-rs `Author` data types.
    #[error(transparent)]
    Author(#[from] AuthorError),

    /// Error returned from validating p2panda-rs `Hash` data types.
    #[error(transparent)]
    Hash(#[from] HashError),

    /// Error returned from validating p2panda-rs `Entry` data types.
    #[error(transparent)]
    Entry(#[from] EntryError),

    /// Error returned from validating p2panda-rs `EntrySigned` data types.
    #[error(transparent)]
    EntrySigned(#[from] EntrySignedError),

    /// Error returned from validating p2panda-rs `Operation` data types.
    #[error(transparent)]
    Operation(#[from] OperationError),

    /// Error returned from validating p2panda-rs `OperationEncoded` data types.
    #[error(transparent)]
    OperationEncoded(#[from] OperationEncodedError),

    /// Error returned from validating p2panda-rs `LogId` data types.
    #[error(transparent)]
    LogId(#[from] LogIdError),

    /// Error returned from validating p2panda-rs `SeqNum` data types.
    #[error(transparent)]
    SeqNum(#[from] SeqNumError),

    /// Error returned from validating Bamboo entries.
    #[error(transparent)]
    Bamboo(#[from] bamboo_rs_core_ed25519_yasmf::verify::Error),
}
