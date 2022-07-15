// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module contains items for generating GraphQL type definitions used in the client API. This
//! includes both types generated for application schemas and static types used in the GraphQL
//! schema.

mod document_meta_type;
mod query_type;
mod schema_fields_type;
mod schema_type;
mod utils;

pub use document_meta_type::DocumentMetaType;
pub use schema_fields_type::SchemaFieldsType;
pub use schema_type::SchemaType;
