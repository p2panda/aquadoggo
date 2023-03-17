// SPDX-License-Identifier: AGPL-3.0-or-later

use std::num::NonZeroU64;

const DEFAULT_PAGE_SIZE: u64 = 10;

// @TODO: Probably this should be a trait
pub type Cursor = String;

#[derive(Debug, Clone)]
pub struct Pagination {
    first: NonZeroU64,
    after: Option<Cursor>,
}

impl Pagination {
    pub fn new(first: NonZeroU64, after: Option<&Cursor>) -> Self {
        Self {
            first,
            after: after.cloned(),
        }
    }
}

impl Default for Pagination {
    fn default() -> Self {
        Self {
            // Unwrap here because we know that the default is non-zero
            first: NonZeroU64::new(DEFAULT_PAGE_SIZE).unwrap(),
            after: None,
        }
    }
}
