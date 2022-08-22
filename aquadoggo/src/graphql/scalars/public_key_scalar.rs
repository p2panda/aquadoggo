// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;
use std::str::FromStr;

use async_graphql::{InputValueError, Scalar, ScalarType, Value};
use p2panda_rs::identity::Author;

/// Public key that signed the entry.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PublicKeyScalar(Author);

#[Scalar(name = "PublicKey")]
impl ScalarType for PublicKeyScalar {
    fn parse(value: Value) -> async_graphql::InputValueResult<Self> {
        match &value {
            Value::String(str_value) => {
                let panda_value = Author::from_str(str_value)?;
                Ok(PublicKeyScalar(panda_value))
            }
            _ => Err(InputValueError::expected_type(value)),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.as_str().to_string())
    }
}

impl From<Author> for PublicKeyScalar {
    fn from(author: Author) -> Self {
        Self(author)
    }
}

impl From<PublicKeyScalar> for Author {
    fn from(public_key: PublicKeyScalar) -> Author {
        public_key.0
    }
}

impl Display for PublicKeyScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
