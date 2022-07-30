// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module contains dynamic type definitions for the GraphQL client api.
//!
//! All dynamic type definitions are inserted from the `OutputType` implementation in the
//! [`dynamic_output_type`] module.

mod document_fields_type;
mod document_meta_type;
mod document_type;
mod dynamic_output_type;
#[cfg(test)]
mod tests;
mod utils;

pub use document_fields_type::DocumentFieldsType;
pub use document_meta_type::DocumentMetaType;
pub use document_type::DocumentType;
