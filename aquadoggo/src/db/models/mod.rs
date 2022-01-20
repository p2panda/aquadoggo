// SPDX-License-Identifier: AGPL-3.0-or-later

mod document;
mod entry;
mod log;

pub use self::log::Log;
pub use entry::Entry;
pub use document::write_document;
