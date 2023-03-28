// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::anyhow;
use dynamic_graphql::{Error, Result, Scalar, ScalarValue, Value};
use p2panda_rs::operation::EncodedOperation;
use serde::{Deserialize, Serialize};

/// Entry payload and p2panda operation, CBOR bytes encoded as a hexadecimal string.
#[derive(Scalar, Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[graphql(name = "EncodedOperation",validator(validate))]
pub struct EncodedOperationScalar(EncodedOperation);

impl ScalarValue for EncodedOperationScalar {
    fn from_value(value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        match &value {
            Value::String(str_value) => {
                let bytes = hex::decode(str_value).map_err(|e| anyhow!(e.to_string()))?;
                Ok(EncodedOperationScalar(EncodedOperation::from_bytes(&bytes)))
            }
            _ => Err(Error::new(format!(
                "Expected a valid encoded operation, found: {value}"
            ))),
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

/// Validation method used internally in `async-graphql` to check scalar values passed into the
/// public api. 
fn validate(value: &Value) -> bool {
    EncodedOperationScalar::from_value(value.to_owned()).is_ok()
}
