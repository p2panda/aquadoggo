// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::entry::{EntryError, EntrySignedError};
use p2panda_rs::hash::HashError;
use p2panda_rs::message::{MessageEncodedError, MessageError};
use p2panda_rs::identity::AuthorError;

/// A specialized `Result` type for the node.
pub type Result<T> = anyhow::Result<T, Error>;

/// Represents all the ways a method can fail within the node.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Error returned from validating p2panda-rs `Author` data types.
    #[error(transparent)]
    AuthorValidation(#[from] AuthorError),

    /// Error returned from validating p2panda-rs `Hash` data types.
    #[error(transparent)]
    HashValidation(#[from] HashError),

    /// Error returned from validating p2panda-rs `Entry` data types.
    #[error(transparent)]
    EntryValidation(#[from] EntryError),

    /// Error returned from validating p2panda-rs `EntrySigned` data types.
    #[error(transparent)]
    EntrySignedValidation(#[from] EntrySignedError),

    /// Error returned from validating p2panda-rs `Message` data types.
    #[error(transparent)]
    MessageValidation(#[from] MessageError),

    /// Error returned from validating p2panda-rs `MessageEncoded` data types.
    #[error(transparent)]
    MessageEncodedValidation(#[from] MessageEncodedError),

    /// Error returned from validating Bamboo entries.
    #[error(transparent)]
    BambooValidation(#[from] bamboo_rs_core_ed25519_yasmf::verify::Error),

    /// Error returned from `panda_publishEntry` RPC method.
    #[error(transparent)]
    PublishEntryValidation(#[from] crate::rpc::PublishEntryError),

    /// Error returned from the database.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}
