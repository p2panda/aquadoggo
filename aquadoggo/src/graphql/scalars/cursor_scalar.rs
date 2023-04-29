// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;
use std::str::FromStr;

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
            Value::String(str_value) => str_value.parse(),
            _ => Err(Error::new(format!("Expected a valid cursor, got: {value}"))),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.encode())
    }
}

impl FromStr for CursorScalar {
    type Err = Error;

    fn from_str(str_value: &str) -> std::result::Result<Self, Self::Err> {
        let cursor = PaginationCursor::decode(str_value)?;
        Ok(CursorScalar(cursor))
    }
}

impl Display for CursorScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<CursorScalar> for PaginationCursor {
    fn from(cursor: CursorScalar) -> Self {
        cursor.0
    }
}

/// Validation method used internally in `async-graphql` to check scalar values passed into the
/// public api.
fn validate(value: &Value) -> bool {
    CursorScalar::from_value(value.to_owned()).is_ok()
}
