// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;

use anyhow::{Error, Result};
use p2panda_rs::entry::LogId;
use p2panda_rs::identity::PublicKey;
use serde::Deserialize;

/// Configuration for the replication service.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ReplicationConfiguration {
    /// How often to connect to remote nodes for replication.
    pub connection_interval_seconds: u64,

    /// The addresses of remote peers to replicate from.
    pub remote_peers: Vec<String>,

    /// The public_keys to replicate and their log ids.
    pub public_keys_to_replicate: Vec<PublicKeyToReplicate>,
}

impl Default for ReplicationConfiguration {
    fn default() -> Self {
        Self {
            connection_interval_seconds: 30,
            remote_peers: Vec::new(),
            public_keys_to_replicate: Vec::new(),
        }
    }
}

/// Intermediate data type to configure which log ids of what public keys should be replicated.
#[derive(Debug, Clone, Deserialize)]
pub struct PublicKeyToReplicate(PublicKey, Vec<LogId>);

impl PublicKeyToReplicate {
    /// Returns public key to be replicated.
    pub fn public_key(&self) -> &PublicKey {
        &self.0
    }

    /// Returns log ids from this public key.
    pub fn log_ids(&self) -> &Vec<LogId> {
        &self.1
    }
}

impl TryFrom<(String, Vec<u64>)> for PublicKeyToReplicate {
    type Error = Error;

    fn try_from(value: (String, Vec<u64>)) -> Result<Self, Self::Error> {
        let public_key = PublicKey::new(&value.0)?;
        let log_ids = value.1.into_iter().map(LogId::new).collect();

        Ok(Self(public_key, log_ids))
    }
}
