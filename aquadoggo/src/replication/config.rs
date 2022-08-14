// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;
use std::str::FromStr;

use anyhow::{Error, Result};
use p2panda_rs::entry::LogId;
use p2panda_rs::identity::Author;
use p2panda_rs::schema::SchemaId;
use serde::Deserialize;

/// Configuration for the replication service.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ReplicationConfiguration {
    /// How often to connect to remote nodes for replication.
    pub connection_interval_seconds: u64,

    /// The addresses of remote peers to replicate from.
    pub remote_peers: Vec<String>,

    /// The authors to replicate and their log ids.
    pub replicate_by_author: Option<Vec<AuthorToReplicate>>,

    /// The schema ids of schemas to replicate.
    pub replicate_by_schema: Option<Vec<SchemaToReplicate>>,
}

impl Default for ReplicationConfiguration {
    fn default() -> Self {
        Self {
            connection_interval_seconds: 30,
            remote_peers: Vec::new(),
            replicate_by_author: Some(Vec::new()),
            replicate_by_schema: Some(Vec::new()),
        }
    }
}

/// Intermediate data type to configure which log ids of what authors should be replicated.
#[derive(Debug, Clone, Deserialize)]
pub struct AuthorToReplicate(Author, Vec<LogId>);

impl AuthorToReplicate {
    /// Returns author to be replicated.
    pub fn author(&self) -> &Author {
        &self.0
    }

    /// Returns log ids from author.
    pub fn log_ids(&self) -> &Vec<LogId> {
        &self.1
    }
}

impl TryFrom<(String, Vec<u64>)> for AuthorToReplicate {
    type Error = Error;

    fn try_from(value: (String, Vec<u64>)) -> Result<Self, Self::Error> {
        let author = Author::new(&value.0)?;
        let log_ids = value.1.into_iter().map(LogId::new).collect();

        Ok(Self(author, log_ids))
    }
}

/// Replication configuration for a single schema.
///
/// Currently only contains the schema's id.
#[derive(Debug, Clone, Deserialize)]
pub struct SchemaToReplicate(SchemaId);

impl SchemaToReplicate {
    /// Access the configured schema id.
    pub fn schema_id(&self) -> &SchemaId {
        &self.0
    }
}

impl TryFrom<String> for SchemaToReplicate {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let schema_id = SchemaId::from_str(&value)?;

        Ok(Self(schema_id))
    }
}
