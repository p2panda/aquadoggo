// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;
use std::str::FromStr;

use dynamic_graphql::{Error, Result, Scalar, ScalarValue, Value};
use p2panda_rs::document::DocumentId;

/// The id of a p2panda document.
#[derive(Scalar, Clone, Debug, Eq, PartialEq)]
#[graphql(name = "DocumentId", validator(validate))]
pub struct DocumentIdScalar(DocumentId);

impl ScalarValue for DocumentIdScalar {
    fn from_value(value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        match &value {
            Value::String(str_value) => {
                let document_id = DocumentId::from_str(str_value)?;
                Ok(DocumentIdScalar(document_id))
            }
            _ => Err(Error::new(format!(
                "Expected a valid document id, got: {value}"
            ))),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.as_str().to_string())
    }
}

impl From<&DocumentId> for DocumentIdScalar {
    fn from(document_id: &DocumentId) -> Self {
        Self(document_id.clone())
    }
}

impl From<&DocumentIdScalar> for DocumentId {
    fn from(document_id: &DocumentIdScalar) -> DocumentId {
        document_id.0.clone()
    }
}

impl Display for DocumentIdScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_str())
    }
}

/// Validation method used internally in `async-graphql` to check scalar values passed into the
/// public api. 
fn validate(value: &Value) -> bool {
    DocumentIdScalar::from_value(value.to_owned()).is_ok()
}
