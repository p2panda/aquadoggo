// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::anyhow;
use dynamic_graphql::{Error, Result, Scalar, ScalarValue, Value};
use p2panda_rs::entry::EncodedEntry;
use serde::{Deserialize, Serialize};

/// Signed bamboo entry, encoded as a hexadecimal string.
#[derive(Scalar, Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[graphql(name = "EntryEncoded")]
pub struct EncodedEntryScalar(EncodedEntry);

impl ScalarValue for EncodedEntryScalar {
    fn from_value(value: Value) -> Result<Self> {
        match &value {
            Value::String(str_value) => {
                let bytes = hex::decode(str_value).map_err(|e| anyhow!(e.to_string()))?;
                Ok(EncodedEntryScalar(EncodedEntry::from_bytes(&bytes)))
            }
            _ => Err(Error::new(format!(
                "Expected a valid encoded entry, found: {value}"
            ))),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

impl From<EncodedEntry> for EncodedEntryScalar {
    fn from(entry: EncodedEntry) -> Self {
        Self(entry)
    }
}

impl From<EncodedEntryScalar> for EncodedEntry {
    fn from(entry: EncodedEntryScalar) -> EncodedEntry {
        entry.0
    }
}
