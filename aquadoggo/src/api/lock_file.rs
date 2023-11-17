// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use p2panda_rs::entry::EncodedEntry;
use p2panda_rs::hash::Hash;
use p2panda_rs::operation::EncodedOperation;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Serializable format holding encoded and signed p2panda operations and entries.
///
/// ```toml
/// version = 1
///
/// [[commits]]
/// entry_hash = "..."
/// entry = "..."
/// operation = "..."
///
/// [[commits]]
/// entry_hash = "..."
/// entry = "..."
/// operation = "..."
///
/// # ...
/// ```
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LockFile {
    /// Version of this lock file.
    pub version: LockFileVersion,

    /// List of commits holding the signed operation and entry data.
    pub commits: Option<Vec<Commit>>,
}

/// Known versions of lock file format.
#[derive(Debug, Clone)]
pub enum LockFileVersion {
    V1,
}

impl LockFileVersion {
    /// Returns the operation version encoded as u64.
    pub fn as_u64(&self) -> u64 {
        match self {
            LockFileVersion::V1 => 1,
        }
    }
}

impl Serialize for LockFileVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.as_u64())
    }
}

impl<'de> Deserialize<'de> for LockFileVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let version = u64::deserialize(deserializer)?;

        match version {
            1 => Ok(LockFileVersion::V1),
            _ => Err(serde::de::Error::custom(format!(
                "unsupported lock file version {}",
                version
            ))),
        }
    }
}

/// Single commit with encoded entry and operation pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Commit {
    /// Hash of the entry.
    pub entry_hash: Hash,

    /// Encoded and signed p2panda entry.
    pub entry: EncodedEntry,

    /// Encoded p2panda operation.
    pub operation: EncodedOperation,
}
