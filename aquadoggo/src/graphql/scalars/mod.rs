// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module contains GraphQL scalar types wrapping p2panda core types.
//!
//! We use a naming convention of appending the item's GraphQL type (e.g. `Scalar`) when a p2panda
//! item of the exact same name is being wrapped.
mod document_id_scalar;
mod document_view_id_scalar;
mod encoded_entry;
mod encoded_operation;
mod entry_hash;
mod log_id;
mod public_key;
mod seq_num;

pub use document_id_scalar::DocumentIdScalar;
pub use document_view_id_scalar::DocumentViewIdScalar;
pub use encoded_entry::EncodedEntry;
pub use encoded_operation::EncodedOperation;
pub use entry_hash::EntryHash;
pub use log_id::LogId;
pub use public_key::PublicKey;
pub use seq_num::SeqNum;
