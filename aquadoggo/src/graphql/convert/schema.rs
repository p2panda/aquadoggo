// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryInto;

use p2panda_rs::hash::{Hash, HASH_SIZE};
use thiserror::Error;

pub const MAX_SCHEMA_NAME: usize = 64;
pub const SCHEMA_NAME_SEPARATOR: &str = "_";

#[derive(Error, Debug)]
#[allow(missing_copy_implementations)]
pub enum SchemaParseError {
    #[error("Schema name is too long with {0} characters ({1} allowed)")]
    TooLong(usize, usize),

    #[error("Name and / or hash field is missing in string")]
    MissingFields,

    #[error(transparent)]
    InvalidHash(#[from] p2panda_rs::hash::HashError),
}

/// Parsed reference of a schema.
///
/// As per specification a schema is referenced by its name and hash in a query.
#[derive(Debug)]
pub struct Schema {
    /// Given name of this schema.
    name: String,

    /// Hash of the schema.
    hash: Hash,
}

impl Schema {
    /// Parsing a schema string.
    pub fn parse(str: &str) -> Result<Self, SchemaParseError> {
        // Allowed schema length is the maximum length of the schema name defined by the p2panda
        // specification, the length of the separator and hash size times two because of its
        // hexadecimal encoding.
        let max_len = MAX_SCHEMA_NAME + SCHEMA_NAME_SEPARATOR.len() + (HASH_SIZE * 2);
        if str.len() > max_len {
            return Err(SchemaParseError::TooLong(str.len(), max_len));
        }

        // Split the string by the separator and return the iterator in reversed form
        let mut fields = str.rsplitn(2, SCHEMA_NAME_SEPARATOR);

        // Since we start parsing the string from the back, we look at the hash first
        let hash = fields
            .next()
            .ok_or(SchemaParseError::MissingFields)?
            .try_into()?;

        // Finally parse the name of the given schema
        let name = fields
            .next()
            .ok_or(SchemaParseError::MissingFields)?
            .to_string();

        Ok(Self { name, hash })
    }

    /// Returns name of SQL table holding document views of this schema.
    pub fn table_name(&self) -> String {
        format!(
            "{}{}{}",
            self.name,
            SCHEMA_NAME_SEPARATOR,
            self.hash.as_str()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::Schema;

    #[test]
    fn parse_schema() {
        let schema = Schema::parse(
            "festivalevents_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b",
        )
        .unwrap();
        assert_eq!(schema.name, "festivalevents");
        assert_eq!(
            schema.hash.as_str(),
            "0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
        );

        let schema = Schema::parse(
            "mul_tiple_separators_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b",
        )
        .unwrap();
        assert_eq!(schema.name, "mul_tiple_separators");
        assert_eq!(
            schema.hash.as_str(),
            "0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
        );
    }

    #[test]
    fn invalid_schema() {
        // Missing name
        assert!(Schema::parse("_0020c6f23dbdc3b5c7b9ab46293111c48fc78b").is_err());

        // Invalid hash (wrong encoding)
        assert!(Schema::parse("test_thisisnotahash").is_err());

        // Separator missing
        assert!(Schema::parse(
            "withoutseparator0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
        )
        .is_err());

        // Invalid hash (too short)
        assert!(Schema::parse("invalid_hash_0020c6f23dbdc3b5c7b9ab46293111c48fc78b").is_err());

        // Schema name exceeds limit
        assert!(Schema::parse("too_long_name_with_many_characters_breaking_the_64_characters_limit_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b").is_err());
    }
}
