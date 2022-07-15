// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module contains type definitions for the GraphQL client api, which are either implemented
//! as `async_graphql` objects or as dynamic type definitions for p2panda schemas.
//!
//! All dynamic type definitions are inserted from the `OutputType` implementation in the
//! [`dynamic_query_type`] module.

mod document_fields_type;
mod document_meta_type;
mod document_type;
mod dynamic_output_type;
mod next_entry_arguments_type;
mod utils;

pub use document_fields_type::DocumentFieldsType;
pub use document_meta_type::DocumentMetaType;
pub use document_type::DocumentType;
pub use next_entry_arguments_type::NextEntryArgumentsType;
