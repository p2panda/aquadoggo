// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use sea_query::{Alias, Expr, Query};

use crate::graphql::convert::{AbstractQuery, Argument, Field, MetaField};

pub fn query_to_sql(root: AbstractQuery) -> Result<String> {
    let mut query = Query::select();

    if root.fields.is_some() {
        for field in root.fields.unwrap() {
            match field {
                Field::Name(name) => {
                    query.column(Alias::new(&name));
                }
                Field::Relation(_) => {
                    panic!("Relations not supported yet");
                }
            }
        }
    }

    if root.meta_fields.is_some() {
        for meta in root.meta_fields.unwrap() {
            match meta {
                MetaField::DocumentHash => {
                    query.column(Alias::new("document"));
                }
            }
        }
    }

    query.from(Alias::new(&root.schema.table_name()));

    if root.arguments.is_some() {
        for arg in root.arguments.unwrap() {
            match arg {
                Argument::DocumentHash(value) => {
                    query.and_where(Expr::col(Alias::new("document")).eq(value.as_str()));
                }
            }
        }
    }

    let sql = query.to_string(sea_query::PostgresQueryBuilder);

    Ok(sql)
}
