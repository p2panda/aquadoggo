// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;
use std::num::NonZeroU64;

pub const DEFAULT_PAGE_SIZE: u64 = 10;

pub trait Cursor: Sized + Clone {
    type Error: Display;

    fn decode(value: &str) -> Result<Self, Self::Error>;

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
