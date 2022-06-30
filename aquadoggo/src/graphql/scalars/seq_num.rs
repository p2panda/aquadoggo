// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::{TryFrom, TryInto};
use std::fmt::Display;
use std::str::FromStr;

use anyhow::Result;
use async_graphql::scalar;
use p2panda_rs::entry::SeqNumError;
use serde::{Deserialize, Serialize};

/// Sequence number of an entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SeqNum(p2panda_rs::entry::SeqNum);

impl SeqNum {
    /// Return sequence number as u64.
    pub fn as_u64(&self) -> u64 {
        self.0.as_u64()
    }

    /// Convert sequence number to string.
    pub fn to_string(&self) -> String {
        self.as_u64().to_string()
    }
}

/// Convert from p2panda types to GraphQL scalars and back.
impl From<p2panda_rs::entry::SeqNum> for SeqNum {
    fn from(seq_num: p2panda_rs::entry::SeqNum) -> Self {
        Self(seq_num)
    }
}

impl From<SeqNum> for p2panda_rs::entry::SeqNum {
    fn from(seq_num: SeqNum) -> p2panda_rs::entry::SeqNum {
        seq_num.0
    }
}

/// Convert from strings to sequence number.
impl FromStr for SeqNum {
    type Err = SeqNumError;

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        let num = u64::from_str(str).map_err(|_| SeqNumError::InvalidU64String)?;
        Ok(Self(p2panda_rs::entry::SeqNum::new(num)?))
    }
}

impl TryFrom<String> for SeqNum {
    type Error = SeqNumError;

    fn try_from(str: String) -> Result<Self, Self::Error> {
        SeqNum::from_str(&str)
    }
}

/// Represent u64 sequence number as string to be able to encode large numbers in GraphQL JSON
/// response.
impl Serialize for SeqNum {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for SeqNum {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let str: String = Deserialize::deserialize(deserializer)?;

        let seq_num: SeqNum = str
            .try_into()
            .map_err(|_| serde::de::Error::custom("Could not parse seq_num string as u64"))?;

        Ok(seq_num)
    }
}

impl Display for SeqNum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_u64())
    }
}

scalar!(SeqNum);

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::SeqNum;

    #[test]
    fn serde_seq_num_as_string() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Value {
            seq_num: SeqNum,
        }

        let val = Value {
            seq_num: p2panda_rs::entry::SeqNum::new(1).unwrap().into(),
        };

        let serialised = serde_json::to_string(&val).unwrap();
        assert_eq!(serialised, "{\"seq_num\":\"1\"}".to_string());
        assert_eq!(val, serde_json::from_str(&serialised).unwrap());
    }
}
