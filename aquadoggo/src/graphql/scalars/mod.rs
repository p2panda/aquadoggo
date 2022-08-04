// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module contains GraphQL scalar types wrapping p2panda core types.
//!
//! All scalar types are safely converted into the corresponding p2panda type when provided as
//! arguments or response values in `async_graphql`.
//!
//! We use a naming convention of appending the item's GraphQL type (e.g. `Scalar`) when a p2panda
//! item of the exact same name is being wrapped.
mod document_id_scalar;
mod document_view_id_scalar;
mod encoded_operation_scalar;
mod entry_hash_scalar;
mod entry_signed_scalar;
mod log_id_scalar;
mod public_key_scalar;
mod seq_num_scalar;

pub use document_id_scalar::DocumentIdScalar;
pub use document_view_id_scalar::DocumentViewIdScalar;
pub use encoded_operation_scalar::EncodedOperationScalar;
pub use entry_hash_scalar::EntryHash;
pub use entry_signed_scalar::EntrySignedScalar;
pub use log_id_scalar::LogIdScalar;
pub use public_key_scalar::PublicKeyScalar;
pub use seq_num_scalar::SeqNumScalar;
