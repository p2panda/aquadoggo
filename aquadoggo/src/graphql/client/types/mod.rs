// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module contains items for generating GraphQL type definitions used in the dynamic part
//! of the client API.
//!
//! All of these type definitions are inserted from the `OutputType` implementation in the
//! [`dynamic_query_type`] module.

mod document_meta_type;
mod dynamic_query_type;
mod schema_fields_type;
mod schema_type;
mod utils;

pub use document_meta_type::DocumentMetaType;
pub use schema_fields_type::SchemaFieldsType;
pub use schema_type::SchemaType;
