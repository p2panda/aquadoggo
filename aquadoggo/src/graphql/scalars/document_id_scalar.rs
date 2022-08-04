// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;

use async_graphql::{InputValueError, InputValueResult, Scalar, ScalarType, Value};
use p2panda_rs::document::DocumentId as PandaDocumentId;
use serde::{Deserialize, Serialize};

/// Id of a p2panda document.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DocumentIdScalar(PandaDocumentId);

#[Scalar]
impl ScalarType for DocumentIdScalar {
    fn parse(value: Value) -> InputValueResult<Self> {
        match &value {
            Value::String(str_value) => {
                let document_id = str_value.as_str().parse::<PandaDocumentId>()?;
                Ok(DocumentIdScalar(document_id))
            }
            _ => Err(InputValueError::expected_type(value)),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.as_str().to_string())
    }
}

impl From<&PandaDocumentId> for DocumentIdScalar {
    fn from(document_id: &PandaDocumentId) -> Self {
        Self(document_id.clone())
    }
}

impl From<&DocumentIdScalar> for PandaDocumentId {
    fn from(document_id: &DocumentIdScalar) -> PandaDocumentId {
        document_id.0.clone()
    }
}

impl Display for DocumentIdScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_str())
    }
}
