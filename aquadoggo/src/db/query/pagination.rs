// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fmt::Display;
use std::num::NonZeroU64;

const DEFAULT_PAGE_SIZE: u64 = 10;

pub trait Cursor: Sized + Clone {
    type Error: Display;

    fn decode(value: &str) -> Result<Self, Self::Error>;

    fn encode(&self) -> String;
}

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
