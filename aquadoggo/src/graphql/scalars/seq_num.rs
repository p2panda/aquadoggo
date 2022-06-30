// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::scalar;
use serde::{Deserialize, Serialize};

/// Sequence number of an entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SeqNum(p2panda_rs::entry::SeqNum);

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

impl Serialize for SeqNum {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Represent u64 sequence number as string to be able to encode large numbers in GraphQL
        // JSON response.
        serializer.serialize_str(&self.0.as_u64().to_string())
    }
}

impl<'de> Deserialize<'de> for SeqNum {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let str: String = Deserialize::deserialize(deserializer)?;

        let seq_num: p2panda_rs::entry::SeqNum = str
            .parse()
            .map_err(|_| serde::de::Error::custom("Could not parse seq_num string as u64"))?;

        Ok(SeqNum(seq_num))
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
