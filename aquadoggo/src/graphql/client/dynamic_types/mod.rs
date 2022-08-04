// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module contains dynamic type definitions for the GraphQL client api.
//!
//! All dynamic type definitions are inserted from the `OutputType` implementation in the
//! [`dynamic_query_output`] module.
mod document;
mod document_fields;
mod document_meta;
mod dynamic_query_output;
#[cfg(test)]
mod tests;
mod utils;

pub use document::Document;
pub use document_fields::DocumentFields;
pub use document_meta::DocumentMeta;
