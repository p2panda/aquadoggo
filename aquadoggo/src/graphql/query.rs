// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{Context, Object, SimpleObject};

use crate::db::models::get_bookmarks;
use crate::db::Pool;
use crate::errors::Result;

#[derive(SimpleObject)]
pub struct BookmarkDocument {
    /// The document's ID
    document: String,

    /// Comma-separated hashes of the document graph's tips, which are required for posting update
    /// or delete operations for this document.
    previous_operations: String,

    /// The materialised fields of this document
    fields: BookmarkFields,
}

#[derive(SimpleObject)]
pub struct BookmarkFields {
    created: String,
    title: String,
    url: String,
}

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn bookmarks<'a>(&self, ctx: &Context<'_>) -> Result<Vec<BookmarkDocument>> {
        let pool = ctx.data::<Pool>().unwrap();
        let bookmarks = get_bookmarks(&pool).await?;

        let results = bookmarks
            .iter()
            .map(|bookmark| {
                let fields = BookmarkFields {
                    created: bookmark.created.clone(),
                    title: bookmark.title.clone(),
                    url: bookmark.url.clone(),
                };

                BookmarkDocument {
                    document: bookmark.document.clone(),
                    previous_operations: bookmark.previous_operations.clone(),
                    fields,
                }
            })
            .collect();

        Ok(results)
    }
}
