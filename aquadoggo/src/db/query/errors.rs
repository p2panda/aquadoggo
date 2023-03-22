// SPDX-License-Identifier: AGPL-3.0-or-later

use thiserror::Error;

/// Validation errors for "abstract" queries.
#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Can't select unknown field '{0}'")]
    SelectFieldUnknown(String),

    #[error("Can't apply ordering on unknown field '{0}'")]
    OrderFieldUnknown(String),

    #[error("Can't apply filter on unknown field '{0}'")]
    FilterFieldUnknown(String),

    #[error("Filter type '{0}' for field '{1}' is not matching schema type '{2}'")]
    FilterInvalidType(String, String, String),

    #[error("Can't apply set filter as field '{0}' is of type boolean")]
    FilterInvalidSet(String),

    #[error("Can't apply interval filter as field '{0}' is not of type string, float or integer")]
    FilterInvalidInterval(String),

    #[error("Can't apply search filter as field '{0}' is not of type string")]
    FilterInvalidSearch(String),
}
