// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;

use async_graphql::{InputValueError, InputValueResult, Scalar, ScalarType, Value};
use p2panda_rs::document::DocumentId;
use serde::{Deserialize, Serialize};

/// Id of a p2panda document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DocumentIdScalar(DocumentId);

#[Scalar]
impl ScalarType for DocumentIdScalar {
    fn parse(value: Value) -> InputValueResult<Self> {
        match &value {
            Value::String(str_value) => {
                let document_id = str_value.as_str().parse::<DocumentId>()?;
                Ok(DocumentIdScalar(document_id))
            }
            _ => Err(InputValueError::expected_type(value)),
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
