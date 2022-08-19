// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::{TryFrom, TryInto};
use std::fmt::Display;
use std::str::FromStr;

use anyhow::Result;
use async_graphql::scalar;
use p2panda_rs::entry::error::SeqNumError;
use p2panda_rs::entry::SeqNum;
use serde::{Deserialize, Serialize};

/// Sequence number of an entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SeqNumScalar(SeqNum);

impl SeqNumScalar {
    /// Return sequence number as u64.
    pub fn as_u64(&self) -> u64 {
        self.0.as_u64()
    }

    /// Convert sequence number to string.
    #[allow(clippy::inherent_to_string_shadow_display)]
    pub fn to_string(self) -> String {
        self.as_u64().to_string()
    }
}

/// Convert from p2panda types to GraphQL scalars and back.
impl From<SeqNum> for SeqNumScalar {
    fn from(seq_num: SeqNum) -> Self {
        Self(seq_num)
    }
}

impl From<SeqNumScalar> for SeqNum {
    fn from(seq_num: SeqNumScalar) -> SeqNum {
        seq_num.0
    }
}

/// Convert from strings to sequence number.
impl FromStr for SeqNumScalar {
    type Err = SeqNumError;

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        let num = u64::from_str(str).map_err(|_| SeqNumError::InvalidU64String)?;
        Ok(Self(SeqNum::new(num)?))
    }
}

impl TryFrom<String> for SeqNumScalar {
    type Error = SeqNumError;

    fn try_from(str: String) -> Result<Self, Self::Error> {
        SeqNumScalar::from_str(&str)
    }
}

/// Represent u64 sequence number as string to be able to encode large numbers in GraphQL JSON
/// response.
impl Serialize for SeqNumScalar {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for SeqNumScalar {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let str: String = Deserialize::deserialize(deserializer)?;

        let seq_num: SeqNumScalar = str
            .try_into()
            .map_err(|_| serde::de::Error::custom("Could not parse seq_num string as u64"))?;

        Ok(seq_num)
    }
}

impl Display for SeqNumScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_u64())
    }
}

scalar!(SeqNumScalar);

#[cfg(test)]
mod tests {
    use p2panda_rs::entry::SeqNum;
    use serde::{Deserialize, Serialize};

    use super::SeqNumScalar;

    #[test]
    fn serde_seq_num_as_string() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Value {
            seq_num: SeqNumScalar,
        }

        let val = Value {
            seq_num: SeqNum::new(1).unwrap().into(),
        };

        let serialised = serde_json::to_string(&val).unwrap();
        assert_eq!(serialised, "{\"seq_num\":\"1\"}".to_string());
        assert_eq!(val, serde_json::from_str(&serialised).unwrap());
    }
}
