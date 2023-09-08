// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;

use dynamic_graphql::{Error, Result, Scalar, ScalarValue, Value};
use serde::Serialize;

/// Bytes encoded as a hexadecimal string.
#[derive(Scalar, Clone, Debug, Eq, PartialEq, Serialize)]
#[graphql(name = "HexBytes", validator(validate))]
pub struct HexBytesScalar(String);

impl ScalarValue for HexBytesScalar {
    fn from_value(value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        match &value {
            Value::String(value) => {
                hex::decode(value)?;
                Ok(HexBytesScalar(value.to_string()))
            }
            _ => Err(Error::new(format!("Expected hex string, found: {value}"))),
        }
    }

    fn to_value(&self) -> Value {
        Value::Binary(self.0.clone().into())
    }
}

impl From<HexBytesScalar> for String {
    fn from(hash: HexBytesScalar) -> Self {
        hash.0
    }
}

impl From<String> for HexBytesScalar {
    fn from(vec: String) -> Self {
        Self(vec)
    }
}

impl From<HexBytesScalar> for Value {
    fn from(entry: HexBytesScalar) -> Self {
        ScalarValue::to_value(&entry)
    }
}

impl Display for HexBytesScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let hex = hex::encode(&self.0);
        write!(f, "{}", hex)
    }
}

/// Validation method used internally in `async-graphql` to check scalar values passed into the
/// public api.
fn validate(value: &Value) -> bool {
    HexBytesScalar::from_value(value.to_owned()).is_ok()
}
