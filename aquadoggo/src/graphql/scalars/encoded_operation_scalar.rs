// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{InputValueError, InputValueResult, Scalar, ScalarType, Value};
use p2panda_rs::operation::OperationEncoded;
use serde::{Deserialize, Serialize};

/// Entry payload and p2panda operation, CBOR bytes encoded as a hexadecimal string.
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug)]
pub struct EncodedOperationScalar(OperationEncoded);

#[Scalar]
impl ScalarType for EncodedOperationScalar {
    fn parse(value: Value) -> InputValueResult<Self> {
        match &value {
            Value::String(str_value) => {
                let panda_value = OperationEncoded::new(str_value)?;
                Ok(EncodedOperationScalar(panda_value))
            }
            _ => Err(InputValueError::expected_type(value)),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.as_str().to_string())
    }
}

impl From<OperationEncoded> for EncodedOperationScalar {
    fn from(operation: OperationEncoded) -> Self {
        Self(operation)
    }
}

impl From<EncodedOperationScalar> for OperationEncoded {
    fn from(operation: EncodedOperationScalar) -> OperationEncoded {
        operation.0
    }
}
