// SPDX-License-Identifier: AGPL-3.0-or-later

//! Dynamic type definitions for the client api.
//!
//! All dynamic type definitions are inserted from the `OutputType` implementation in the
//! [`dynamic_query_output`] module.
pub(crate) mod document;
mod document_fields;
pub(crate) mod document_meta;
pub(crate) mod dynamic_query_output;
#[cfg(test)]
mod tests;
mod utils;

pub use document::Document;
pub use document_fields::DocumentFields;
pub use document_meta::DocumentMeta;
