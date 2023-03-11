// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;
use std::str::FromStr;

use dynamic_graphql::{Error, Result, Scalar, ScalarValue, Value};
use p2panda_rs::entry::LogId;
use serde::{Deserialize, Serialize};

/// Log id of a bamboo entry.
#[derive(Scalar, Clone, Copy, Eq, PartialEq, Debug)]
#[graphql(name = "LogId")]
pub struct LogIdScalar(LogId);

impl ScalarValue for LogIdScalar {
    fn from_value(value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        match &value {
            Value::String(str_value) => {
                let log_id = LogId::from_str(str_value)?;
                Ok(LogIdScalar(log_id))
            }
            _ => Err(Error::new(format!(
                "Expected a valid log id, found: {value}"
            ))),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.as_u64().to_string())
    }
}

impl From<LogId> for LogIdScalar {
    fn from(log_id: LogId) -> Self {
        Self(log_id)
    }
}

impl From<LogIdScalar> for LogId {
    fn from(log_id: LogIdScalar) -> LogId {
        log_id.0
    }
}

impl Serialize for LogIdScalar {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Represent u64 log id as string to be able to encode large numbers in GraphQL JSON
        // response.
        serializer.serialize_str(&self.0.as_u64().to_string())
    }
}

impl<'de> Deserialize<'de> for LogIdScalar {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let str: String = Deserialize::deserialize(deserializer)?;

        let log_id: LogId = str
            .parse()
            .map_err(|_| serde::de::Error::custom("Could not parse log_id string as u64"))?;

        Ok(LogIdScalar(log_id))
    }
}

impl Display for LogIdScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_u64())
    }
}

#[cfg(test)]
mod tests {
    use dynamic_graphql::{ScalarValue, Value};
    use p2panda_rs::entry::LogId;
    use serde::{Deserialize, Serialize};

    use super::LogIdScalar;

    #[test]
    fn serde_log_id_as_string() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Value {
            log_id: LogIdScalar,
        }

        let val = Value {
            log_id: LogId::default().into(),
        };

        let serialised = serde_json::to_string(&val).unwrap();
        assert_eq!(serialised, "{\"log_id\":\"0\"}".to_string());
        assert_eq!(val, serde_json::from_str(&serialised).unwrap());
    }

    #[test]
    fn scalar_type() {
        // Convert to gql value
        let value = LogId::default();
        let scalar_value: LogIdScalar = value.into();
        let gql_value: Value = scalar_value.to_value();

        // Convert back
        let converted_value = LogIdScalar::from_value(gql_value).unwrap().into();
        assert_eq!(value, converted_value);

        // Convert invalid type
        let invalid_conversion = LogIdScalar::from_value(Value::Boolean(true));
        assert!(invalid_conversion.is_err())
    }
}
