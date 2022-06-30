// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;

use async_graphql::scalar;
use serde::{Deserialize, Serialize};

/// Log id of a bamboo entry.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct LogId(p2panda_rs::entry::LogId);

impl From<p2panda_rs::entry::LogId> for LogId {
    fn from(log_id: p2panda_rs::entry::LogId) -> Self {
        Self(log_id)
    }
}

impl From<LogId> for p2panda_rs::entry::LogId {
    fn from(log_id: LogId) -> p2panda_rs::entry::LogId {
        log_id.0
    }
}

impl Serialize for LogId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Represent u64 log id as string to be able to encode large numbers in GraphQL JSON
        // response.
        serializer.serialize_str(&self.0.as_u64().to_string())
    }
}

impl<'de> Deserialize<'de> for LogId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let str: String = Deserialize::deserialize(deserializer)?;

        let log_id: p2panda_rs::entry::LogId = str
            .parse()
            .map_err(|_| serde::de::Error::custom("Could not parse log_id string as u64"))?;

        Ok(LogId(log_id))
    }
}

impl Display for LogId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_u64())
    }
}

scalar!(LogId);

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::LogId;

    #[test]
    fn serde_log_id_as_string() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Value {
            log_id: LogId,
        }

        let val = Value {
            log_id: p2panda_rs::entry::LogId::new(1).into(),
        };

        let serialised = serde_json::to_string(&val).unwrap();
        assert_eq!(serialised, "{\"log_id\":\"1\"}".to_string());
        assert_eq!(val, serde_json::from_str(&serialised).unwrap());
    }
}
