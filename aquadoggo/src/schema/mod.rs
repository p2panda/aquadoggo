// SPDX-License-Identifier: AGPL-3.0-or-later

mod schema_provider;
mod static_schema_provider;

pub use schema_provider::SchemaProvider;
pub use static_schema_provider::{load_static_schemas, save_static_schemas, StaticLeak};
