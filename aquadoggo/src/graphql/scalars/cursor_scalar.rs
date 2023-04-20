// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;

use anyhow::{anyhow, Error as AnyhowError};
use async_graphql::{Error, Result};
use dynamic_graphql::{Scalar, ScalarValue, Value};

use crate::db::query::Cursor;
use crate::db::stores::PaginationCursor;

/// The cursor used in paginated queries.
#[derive(Scalar, Clone, Debug, Eq, PartialEq)]
#[graphql(name = "Cursor", validator(validate))]
pub struct CursorScalar(PaginationCursor);

impl ScalarValue for CursorScalar {
    fn from_value(value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        match &value {
            Value::String(str_value) => Ok(Self(PaginationCursor::decode(str_value)?)),
            _ => Err(Error::new(format!("Expected a valid cursor, got: {value}"))),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.encode())
    }
}

impl Display for CursorScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.encode())
    }
}

impl Cursor for CursorScalar {
    type Error = AnyhowError;

    fn decode(value: &str) -> Result<Self, Self::Error> {
        let value = Value::String(value.to_string());

        Self::from_value(value).map_err(|err| anyhow!(err.message.as_str().to_owned()))
    }

    fn encode(&self) -> String {
        self.0.encode()
    }
}

impl From<&PaginationCursor> for CursorScalar {
    fn from(cursor: &PaginationCursor) -> Self {
        Self(cursor.clone())
    }
}

impl From<&CursorScalar> for PaginationCursor {
    fn from(cursor: &CursorScalar) -> PaginationCursor {
        cursor.0.clone()
    }
}

/// Validation method used internally in `async-graphql` to check scalar values passed into the
/// public api.
fn validate(value: &Value) -> bool {
    CursorScalar::from_value(value.to_owned()).is_ok()
}
