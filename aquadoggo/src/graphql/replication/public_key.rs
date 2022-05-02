use async_graphql::*;
use base64::{decode, encode};

pub struct PublicKey(Vec<u8>);

#[Scalar]
impl ScalarType for PublicKey {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = value {
            let bytes = decode(value)?;
            Ok(Self(bytes))
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(encode(&self.0))
    }
}
