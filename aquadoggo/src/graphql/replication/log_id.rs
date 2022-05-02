use async_graphql::*;

pub struct LogId(u64);

#[Scalar]
impl ScalarType for LogId {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = value {
            let n: u64 = u64::from_str_radix(&value, 10)?;
            Ok(Self(n))
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}
