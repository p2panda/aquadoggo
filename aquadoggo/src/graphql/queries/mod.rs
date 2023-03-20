// SPDX-License-Identifier: AGPL-3.0-or-later

mod next_args;
mod document;
mod all_documents;

pub use next_args::build_next_args_query;
pub use document::build_document_query;
pub use all_documents::build_all_documents_query;