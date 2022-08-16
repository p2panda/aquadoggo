// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{InputValueError, InputValueResult, Scalar, ScalarType, Value};
use p2panda_rs::operation::EncodedOperation;
use p2panda_rs::serde::deserialize_hex;
use serde::{Deserialize, Serialize};

/// Entry payload and p2panda operation, CBOR bytes encoded as a hexadecimal string.
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug)]
pub struct EncodedOperationScalar(EncodedOperation);

#[Scalar]
impl ScalarType for EncodedOperationScalar {
    fn parse(value: Value) -> InputValueResult<Self> {
        match &value {
            Value::String(str_value) => {
                //@TODO: I'm sure this isn't the best way to do this...
                // also, I'm not sure why `::from_str` is visible here when it should
                // be behind the testing flag in p2panda-rs ;-p
                Ok(EncodedOperationScalar(EncodedOperation::from_str(
                    str_value,
                )))
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
