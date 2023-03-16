// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;
use std::str::FromStr;

use dynamic_graphql::{Error, Result, Scalar, ScalarValue, Value};
use p2panda_rs::hash::Hash;
use serde::Serialize;

/// Hash of a signed bamboo entry.
#[derive(Scalar, Clone, Debug, Eq, PartialEq, Serialize)]
#[graphql(name = "EntryHash")]
pub struct EntryHashScalar(Hash);

impl ScalarValue for EntryHashScalar {
    fn from_value(value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        match &value {
            Value::String(str_value) => {
                let hash = Hash::from_str(str_value)?;
                Ok(EntryHashScalar(hash))
            }
            _ => Err(Error::new(format!(
                "Expected a valid entry hash, found: {value}"
            ))),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

impl From<EntryHashScalar> for Hash {
    fn from(hash: EntryHashScalar) -> Self {
        hash.0
    }
}

impl From<Hash> for EntryHashScalar {
    fn from(hash: Hash) -> Self {
        Self(hash)
    }
}

impl From<EntryHashScalar> for Value {
    fn from(entry: EntryHashScalar) -> Self {
        ScalarValue::to_value(&entry)
    }
}

impl Display for EntryHashScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::document::DocumentViewId;

    use super::EntryHashScalar;

    impl From<EntryHashScalar> for DocumentViewId {
        fn from(hash: EntryHashScalar) -> Self {
            hash.0.into()
        }
    }
}
