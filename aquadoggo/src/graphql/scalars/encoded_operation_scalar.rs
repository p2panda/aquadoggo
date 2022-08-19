// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{InputValueError, InputValueResult, Scalar, ScalarType, Value};
use p2panda_rs::operation::EncodedOperation;
use serde::{Deserialize, Serialize};

/// Entry payload and p2panda operation, CBOR bytes encoded as a hexadecimal string.
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug)]
pub struct EncodedOperationScalar(EncodedOperation);

#[Scalar]
impl ScalarType for EncodedOperationScalar {
    fn parse(value: Value) -> InputValueResult<Self> {
        match &value {
            Value::String(str_value) => {
                let bytes = hex::decode(str_value)?;
                Ok(EncodedOperationScalar(EncodedOperation::from_bytes(&bytes)))
            }
            _ => Err(InputValueError::expected_type(value)),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

impl From<EncodedOperation> for EncodedOperationScalar {
    fn from(operation: EncodedOperation) -> Self {
        Self(operation)
    }
}

impl From<EncodedOperationScalar> for EncodedOperation {
    fn from(operation: EncodedOperationScalar) -> EncodedOperation {
        operation.0
    }
}
