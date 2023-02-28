// SPDX-License-Identifier: AGPL-3.0-or-later

//! Structs representing data which has been retrieved from the store.
//!
//! As data coming from the db is trusted we construct these structs without validation. Compared
//! to their respective counterparts in `p2panda-rs` some additional values are also made
//! available. For example, all `StorageOperation`s contain the `DocumentId` of the document they
//! are associated with, this value is not encoded in an plain operation and must be derived from
//! other values stored in the database.
mod document;
mod entry;
mod operation;

pub use document::StorageDocument;
pub use entry::StorageEntry;
pub use operation::StorageOperation;
