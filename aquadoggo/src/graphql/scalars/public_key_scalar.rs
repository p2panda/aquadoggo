// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;
use std::str::FromStr;

use async_graphql::{InputValueError, Scalar, ScalarType, Value};
use p2panda_rs::identity::PublicKey;

/// Public key that signed the entry.
#[derive(Debug, Clone, Eq, PartialEq, Copy)]
pub struct PublicKeyScalar(PublicKey);

#[Scalar(name = "PublicKey")]
impl ScalarType for PublicKeyScalar {
    fn parse(value: Value) -> async_graphql::InputValueResult<Self> {
        match &value {
            Value::String(str_value) => {
                let panda_value = PublicKey::from_str(str_value)?;
                Ok(PublicKeyScalar(panda_value))
            }
            _ => Err(InputValueError::expected_type(value)),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

impl From<PublicKey> for PublicKeyScalar {
    fn from(public_key: PublicKey) -> Self {
        Self(public_key)
    }
}

impl From<PublicKeyScalar> for PublicKey {
    fn from(public_key: PublicKeyScalar) -> PublicKey {
        public_key.0
    }
}

impl Display for PublicKeyScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
