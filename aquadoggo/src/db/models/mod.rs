// SPDX-License-Identifier: AGPL-3.0-or-later

//! Structs representing rows in SQL tables. Needed when coercing results returned from a
//! query using the `sqlx` library.
mod document;
mod entry;
mod log;
mod operation;
mod query;
mod task;
pub mod utils;

pub use self::log::LogHeightRow;
pub use document::{DocumentRow, DocumentViewFieldRow};
pub use entry::EntryRow;
pub use operation::OperationFieldsJoinedRow;
#[cfg(test)]
pub use query::OptionalOwner;
pub use query::QueryRow;
pub use task::TaskRow;
