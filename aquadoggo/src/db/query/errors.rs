// SPDX-License-Identifier: AGPL-3.0-or-later

use thiserror::Error;

/// Validation errors for "abstract" queries.
#[derive(Error, Debug)]
pub enum QueryError {
    /// Selection contains a field which is not part of the given schema.
    #[error("Can't select unknown field '{0}'")]
    SelectFieldUnknown(String),

    /// Filter contains a field which is not part of the given schema.
    #[error("Can't apply filter on unknown field '{0}'")]
    FilterFieldUnknown(String),

    /// Ordering is based on a field which is not part of the given schema.
    #[error("Can't apply ordering on unknown field '{0}'")]
    OrderFieldUnknown(String),

    /// Filter can not be applied to a field of given type.
    #[error("Filter type '{0}' for field '{1}' is not matching schema type '{2}'")]
    FilterInvalidType(String, String, String),

    /// Set filters are not possible for boolean values.
    #[error("Can't apply set filter as field '{0}' is of type boolean")]
    FilterInvalidSet(String),

    /// Interval filters arae not possible for booleans and relations.
    #[error("Can't apply interval filter as field '{0}' is not of type string, float or integer")]
    FilterInvalidInterval(String),

    /// Search filters can only be applied on strings.
    #[error("Can't apply search filter as field '{0}' is not of type string")]
    FilterInvalidSearch(String),
}
