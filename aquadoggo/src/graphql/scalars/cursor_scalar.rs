// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;
use std::str::FromStr;

use anyhow::{anyhow, Error as AnyhowError};
use async_graphql::{Error, Result};
use dynamic_graphql::{Scalar, ScalarValue, Value};
use p2panda_rs::document::DocumentId;

use crate::db::query::Cursor;

/// A cursor used in paginated queries.
#[derive(Scalar, Clone, Debug, Eq, PartialEq)]
#[graphql(name = "Cursor")]
pub struct CursorScalar(String, DocumentId);

impl ScalarValue for CursorScalar {
    fn from_value(value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        match &value {
            Value::String(str_value) => {
                let parts: Vec<String> = str_value.split('_').map(|part| part.to_owned()).collect();

                if parts.len() != 2 {
                    return Err(Error::new("Invalid amount of cursor parts"));
                }

                Ok(Self(
                    parts[0].clone(),
                    DocumentId::from_str(parts[1].as_str())?,
                ))
            }
            _ => Err(Error::new(format!("Expected a valid cursor, got: {value}"))),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.as_str().to_string())
    }
}

impl Display for CursorScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{}", self.0, self.1.as_str())
    }
}

impl Cursor for CursorScalar {
    type Error = AnyhowError;

    fn decode(value: &str) -> Result<Self, Self::Error> {
        let value = Value::String(value.to_string());

        Self::from_value(value).map_err(|err| anyhow!(err.message.as_str().to_owned()))
    }

    fn encode(&self) -> String {
        format!("{}_{}", self.0, self.1)
    }
}
