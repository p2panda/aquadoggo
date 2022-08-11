// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{InputValueError, Scalar, ScalarType, Value};
use p2panda_rs::entry::EntrySigned;
use serde::{Deserialize, Serialize};

/// Signed bamboo entry, encoded as a hexadecimal string.
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug)]
pub struct EntrySignedScalar(EntrySigned);

#[Scalar(name = "EntrySigned")]
impl ScalarType for EntrySignedScalar {
    fn parse(value: Value) -> async_graphql::InputValueResult<Self> {
        match &value {
            Value::String(str_value) => {
                let panda_value = EntrySigned::new(str_value)?;
                Ok(EntrySignedScalar(panda_value))
            }
            _ => Err(InputValueError::expected_type(value)),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.as_str().to_string())
    }
}

impl From<EntrySigned> for EntrySignedScalar {
    fn from(entry: EntrySigned) -> Self {
        Self(entry)
    }
}

impl From<EntrySignedScalar> for EntrySigned {
    fn from(entry: EntrySignedScalar) -> EntrySigned {
        entry.0
    }
}

impl From<EntrySignedScalar> for Value {
    fn from(entry: EntrySignedScalar) -> Self {
        async_graphql::ScalarType::to_value(&entry)
    }
}
