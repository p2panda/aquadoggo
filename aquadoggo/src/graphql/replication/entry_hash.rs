use async_graphql::*;
use base64::{decode, encode};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use std::convert::TryFrom;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(into = "Value")]
#[serde(try_from = "Value")]
pub struct EntryHash(pub Vec<u8>);

#[Scalar]
impl ScalarType for EntryHash {
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

impl From<EntryHash> for Value {
    fn from(entry: EntryHash) -> Self {
        async_graphql::ScalarType::to_value(&entry)
    }
}

#[derive(Snafu, Debug)]
pub enum ConversionError {
    #[snafu(display("Unable to convert value to EntryHash, err: {:?}", err))]
    ConvertError { err: InputValueError<EntryHash> },
}

impl TryFrom<Value> for EntryHash {
    type Error = ConversionError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        async_graphql::ScalarType::parse(value).map_err(|err| ConversionError::ConvertError { err })
    }
}
