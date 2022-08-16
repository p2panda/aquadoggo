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
                //@TODO: I'm sure this isn't the best way to do this...
                // also, I'm not sure why `::new` is visible here when it should
                // be behind the testing flag in p2panda-rs ;-p
                let bytes = deserialize_hex(value)?;
                Ok(EntrySignedScalar(EncodedEntry::new(&bytes)))
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
