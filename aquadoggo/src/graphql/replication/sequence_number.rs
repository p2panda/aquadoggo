use async_graphql::*;
use std::num::NonZeroU64;
use std::convert::TryInto;

pub struct SequenceNumber(NonZeroU64);

#[Scalar]
impl ScalarType for SequenceNumber {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = value {
            let n: u64 = u64::from_str_radix(&value, 10)?;
            let non_zero: NonZeroU64 = n.try_into()?;
            Ok(Self(non_zero))
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}
