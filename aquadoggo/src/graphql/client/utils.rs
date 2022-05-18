// SPDX-License-Identifier: AGPL-3.0-or-later

use core::fmt;

use serde::de::{self, Unexpected, Visitor};

/// Serde visitor for deserialising string representations of u64 values.
pub(crate) struct U64StringVisitor;

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
