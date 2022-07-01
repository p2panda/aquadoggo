// SPDX-License-Identifier: AGPL-3.0-or-later

mod provider;
mod static_provider;

pub use provider::SchemaProvider;
pub use static_provider::{load_static_schemas, save_static_schemas};
