// SPDX-License-Identifier: AGPL-3.0-or-later

mod config;
mod service;
mod replicate_authors;

pub use config::{ReplicationConfiguration, AuthorToReplicate, SchemaToReplicate};
pub use service::replication_service;
