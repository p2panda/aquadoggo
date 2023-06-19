// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::{self, Display};

use p2panda_rs::Human;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Mode {
    LogHeight,
    SetReconciliation,
    Unknown,
}

impl Mode {
    pub fn as_str(&self) -> &str {
        match self {
            Mode::LogHeight => "log-height",
            Mode::SetReconciliation => "set-reconciliation",
            Mode::Unknown => "unknown",
        }
    }

    pub fn as_u64(&self) -> u64 {
        match self {
            Mode::LogHeight => 0,
            Mode::SetReconciliation => 1,
            Mode::Unknown => unreachable!("Can't create an unknown replication mode"),
        }
    }
}

impl From<u64> for Mode {
    fn from(value: u64) -> Self {
        match value {
            0 => Mode::LogHeight,
            1 => Mode::SetReconciliation,
            _ => Mode::Unknown,
        }
    }
}

impl Serialize for Mode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.as_u64())
    }
}

impl<'de> Deserialize<'de> for Mode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mode = u64::deserialize(deserializer)?;
        Ok(mode.into())
    }
}

impl Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_u64())
    }
}

impl Human for Mode {
    fn display(&self) -> String {
        self.as_str().to_owned()
    }
}

#[cfg(test)]
mod tests {
    use ciborium::cbor;

    use p2panda_rs::serde::{deserialize_into, serialize_from, serialize_value};

    use super::Mode;

    #[test]
    fn u64_representation() {
        assert_eq!(Mode::LogHeight.as_u64(), 0);
        assert_eq!(Mode::SetReconciliation.as_u64(), 1);
    }

    #[test]
    fn serialize() {
        let bytes = serialize_from(Mode::LogHeight);
        assert_eq!(bytes, vec![0]);
    }

    #[test]
    fn deserialize() {
        let version: Mode = deserialize_into(&serialize_value(cbor!(1))).unwrap();
        assert_eq!(version, Mode::SetReconciliation);

        // Can not be a string
        let invalid_type = deserialize_into::<Mode>(&serialize_value(cbor!("0")));
        assert!(invalid_type.is_err());
    }
}
