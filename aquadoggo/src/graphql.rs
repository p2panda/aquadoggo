use async_graphql::{Context, EmptyMutation, EmptySubscription, Object, Schema, SimpleObject};

use crate::db::models::get_bookmarks;
use crate::db::Pool;
use crate::errors::Result;

#[derive(SimpleObject)]
pub struct BookmarkDocument {
    document: String,
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
    async fn bookmarks<'a>(&self, ctx: &Context<'a>) -> Result<Vec<BookmarkDocument>> {
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
                    fields,
                    document: bookmark.document.clone(),
                }
            })
            .collect();

        Ok(results)
    }
}

pub fn build_schema(pool: Pool) -> Schema<QueryRoot, EmptyMutation, EmptySubscription> {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(pool)
        .finish()
}
