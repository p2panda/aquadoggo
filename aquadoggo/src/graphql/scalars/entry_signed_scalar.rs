// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{InputValueError, Scalar, ScalarType, Value};
use p2panda_rs::entry::traits::AsEncodedEntry;
use p2panda_rs::entry::EncodedEntry;
use p2panda_rs::serde::deserialize_hex;
use serde::{Deserialize, Serialize};

/// Signed bamboo entry, encoded as a hexadecimal string.
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug)]
pub struct EntrySignedScalar(EncodedEntry);

#[Scalar]
impl ScalarType for EntrySignedScalar {
    fn parse(value: Value) -> async_graphql::InputValueResult<Self> {
        match &value {
            Value::String(str_value) => {
                let bytes = hex::decode(str_value)?;
                Ok(EntrySignedScalar(EncodedEntry::from_bytes(&bytes)))
            }
            _ => Err(InputValueError::expected_type(value)),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

impl From<EncodedEntry> for EntrySignedScalar {
    fn from(entry: EncodedEntry) -> Self {
        Self(entry)
    }
}

impl From<EntrySignedScalar> for EncodedEntry {
    fn from(entry: EntrySignedScalar) -> EncodedEntry {
        entry.0
    }
}

impl From<EntrySignedScalar> for Value {
    fn from(entry: EntrySignedScalar) -> Self {
        async_graphql::ScalarType::to_value(&entry)
    }
}
