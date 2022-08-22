// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::anyhow;
use async_graphql::{InputValueError, InputValueResult, Scalar, ScalarType, Value};
use p2panda_rs::entry::EncodedEntry;
use serde::{Deserialize, Serialize};

/// Signed bamboo entry, encoded as a hexadecimal string.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct EncodedEntryScalar(EncodedEntry);

#[Scalar(name = "EncodedEntry")]
impl ScalarType for EncodedEntryScalar {
    fn parse(value: Value) -> InputValueResult<Self> {
        match &value {
            Value::String(str_value) => {
                let bytes =
                    hex::decode(str_value).map_err(|_| anyhow!("invalid hex encoding in entry"))?;
                Ok(EncodedEntryScalar(EncodedEntry::from_bytes(&bytes)))
            }
            _ => Err(InputValueError::expected_type(value)),
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
