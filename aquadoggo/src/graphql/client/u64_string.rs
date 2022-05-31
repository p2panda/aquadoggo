// SPDX-License-Identifier: AGPL-3.0-or-later

use core::fmt;

use serde::de::{self, Unexpected, Visitor};

/// Serialise log id as strings.
///
/// To be used as a parameter for Serde's `with` field attribute.
pub mod log_id_string_serialisation {
    use p2panda_rs::entry::LogId;
    use serde::{Deserializer, Serializer};

    use super::U64StringVisitor;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<LogId, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u64_val = deserializer.deserialize_string(U64StringVisitor)?;
        Ok(LogId::new(u64_val))
    }

    pub fn serialize<S>(value: &LogId, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.as_u64().to_string())
    }
}

/// Serialise sequence numbers as strings.
///
/// To be used as a parameter for Serde's `with` field attribute.
pub mod seq_num_string_serialisation {
    use p2panda_rs::entry::SeqNum;
    use serde::de::Error;
    use serde::{Deserializer, Serializer};

    use super::U64StringVisitor;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SeqNum, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u64_val = deserializer.deserialize_string(U64StringVisitor)?;
        SeqNum::new(u64_val).map_err(D::Error::custom)
    }

    pub fn serialize<S>(value: &SeqNum, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.as_u64().to_string())
    }
}

/// Serde visitor for deserialising string representations of u64 values.
struct U64StringVisitor;

impl<'de> Visitor<'de> for U64StringVisitor {
    type Value = u64;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("string representation of a u64")
    }

    fn visit_str<E>(self, value: &str) -> Result<u64, E>
    where
        E: de::Error,
    {
        value.parse::<u64>().map_err(|_err| {
            E::invalid_value(Unexpected::Str(value), &"string representation of a u64")
        })
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::entry::{LogId, SeqNum};
    use serde::{Deserialize, Serialize};

    #[test]
    fn log_id() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Value {
            #[serde(with = "super::log_id_string_serialisation")]
            log_id: LogId,
        }

        let val = Value {
            log_id: LogId::new(1),
        };
        let serialised = serde_json::to_string(&val).unwrap();
        assert_eq!(serialised, "{\"log_id\":\"1\"}".to_string());
        assert_eq!(val, serde_json::from_str(&serialised).unwrap());
    }

    #[test]
    fn seq_num() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Value {
            #[serde(with = "super::seq_num_string_serialisation")]
            seq_num: SeqNum,
        }

        let val = Value {
            seq_num: SeqNum::new(1).unwrap(),
        };
        let serialised = serde_json::to_string(&val).unwrap();
        assert_eq!(serialised, "{\"seq_num\":\"1\"}".to_string());
        assert_eq!(val, serde_json::from_str(&serialised).unwrap());
    }
}
