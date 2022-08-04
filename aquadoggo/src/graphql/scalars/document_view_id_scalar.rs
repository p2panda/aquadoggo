// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;

use async_graphql::{InputValueError, InputValueResult, Scalar, ScalarType, Value};
use p2panda_rs::document::DocumentViewId as PandaViewId;
use serde::{Deserialize, Serialize};

/// Document view id as a GraphQL scalar.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DocumentViewIdScalar(PandaViewId);

#[Scalar]
impl ScalarType for DocumentViewIdScalar {
    fn parse(value: Value) -> InputValueResult<Self> {
        match &value {
            Value::String(str_value) => {
                let view_id = str_value.parse::<PandaViewId>()?;
                Ok(DocumentViewIdScalar(view_id))
            }
            _ => Err(InputValueError::expected_type(value)),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.as_str())
    }
}

impl From<&PandaViewId> for DocumentViewIdScalar {
    fn from(value: &PandaViewId) -> Self {
        DocumentViewIdScalar(value.clone())
    }
}

impl From<&DocumentViewIdScalar> for PandaViewId {
    fn from(value: &DocumentViewIdScalar) -> Self {
        // Unwrap because `DocumentViewIdScalar` is always safely intialised.
        PandaViewId::new(value.0.graph_tips()).unwrap()
    }
}

impl Display for DocumentViewIdScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
