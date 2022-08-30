// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;

use async_graphql::{InputValueError, InputValueResult, Scalar, ScalarType, Value};
use p2panda_rs::operation::OperationId;

/// Id of a p2panda document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationIdScalar(OperationId);

#[Scalar]
impl ScalarType for OperationIdScalar {
    fn parse(value: Value) -> InputValueResult<Self> {
        match &value {
            Value::String(str_value) => {
                let operation_id = str_value.as_str().parse::<OperationId>()?;
                Ok(OperationIdScalar(operation_id))
            }
            _ => Err(InputValueError::expected_type(value)),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.as_str().to_string())
    }
}

impl From<&OperationId> for OperationIdScalar {
    fn from(operation_id: &OperationId) -> Self {
        Self(operation_id.clone())
    }
}

impl From<&OperationIdScalar> for OperationId {
    fn from(operation_id: &OperationIdScalar) -> OperationId {
        operation_id.0.clone()
    }
}

impl Display for OperationIdScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_str())
    }
}
