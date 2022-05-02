use async_graphql::*;

pub struct Schema(String);

#[Scalar]
impl ScalarType for Schema {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = value {
            Ok(Self(value))
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.clone())
    }
}
