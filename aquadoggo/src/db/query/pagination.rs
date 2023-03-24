// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;
use std::num::NonZeroU64;

/// Default page size as defined by the p2panda specification.
pub const DEFAULT_PAGE_SIZE: u64 = 25;

/// Traits to define your own cursor implementation which will be used during pagination.
///
/// Cursors are always strings and ideally opaque. The latter can be achieved by for example
/// hashing the original value so now semantics can be derived from the cursor itself.
pub trait Cursor: Sized + Clone {
    /// Error type for failed decoding.
    type Error: Display;

    /// Convert any string cursor back into its original form.
    fn decode(value: &str) -> Result<Self, Self::Error>;

    /// Convert any object into a string cursor.
    fn encode(&self) -> String;
}

#[cfg(test)]
impl Cursor for String {
    type Error = anyhow::Error;

    fn decode(value: &str) -> Result<Self, Self::Error> {
        Ok(value.to_string())
    }

    fn encode(&self) -> String {
        self.clone()
    }
}

/// Pagination settings which can be used further to construct a database query.
///
/// This object represents all required values to allow cursor-based pagination, while the cursor
/// can be externally defined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pagination<C>
where
    C: Cursor,
{
    pub first: NonZeroU64,
    pub after: Option<C>,
}

impl<C> Pagination<C>
where
    C: Cursor,
{
    /// Returns a new instance of pagination settings.
    pub fn new(first: &NonZeroU64, after: Option<&C>) -> Self {
        Self {
            first: *first,
            after: after.cloned(),
        }
    }
}

impl<C> Default for Pagination<C>
where
    C: Cursor,
{
    fn default() -> Self {
        Self {
            // Unwrap here because we know that the default is non-zero
            first: NonZeroU64::new(DEFAULT_PAGE_SIZE).unwrap(),
            after: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::str::FromStr;

    use anyhow::{bail, Error};

    use super::{Cursor, Pagination, DEFAULT_PAGE_SIZE};

    #[test]
    fn create_pagination() {
        assert_eq!(
            Pagination::<String>::new(&NonZeroU64::new(DEFAULT_PAGE_SIZE).unwrap(), None),
            Pagination::<String>::default()
        )
    }

    #[test]
    fn cursor_encoding() {
        #[derive(Clone)]
        struct TestCursor(u64, u64);

        impl Cursor for TestCursor {
            type Error = Error;

            fn decode(value: &str) -> Result<Self, Self::Error> {
                let parts: Vec<u64> = value
                    .split('_')
                    .map(|part| Ok(u64::from_str(part)?))
                    .collect::<Result<Vec<u64>, Self::Error>>()?;

                if parts.len() != 2 {
                    bail!("Invalid amount of parts")
                }

                Ok(Self(parts[0], parts[1]))
            }

            fn encode(&self) -> String {
                format!("{}_{}", self.0, self.1)
            }
        }

        let cursor = TestCursor(22, 21);
        assert_eq!(cursor.encode(), "22_21".to_string());

        assert!(TestCursor::decode("22_21").is_ok());
        assert!(TestCursor::decode("22_21_20").is_err());
        assert!(TestCursor::decode("wrong_22").is_err());
    }
}
