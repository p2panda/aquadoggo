// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use apollo_parser::ast::Document;
use apollo_parser::Parser;
use sea_query::{Alias, Expr, Query};

use crate::graphql::convert::{AbstractQuery, Argument, Field, MetaField};

fn parse_graphql_query(query: &str) -> Result<Document> {
    let parser = Parser::new(query);
    let ast = parser.parse();

    if ast.errors().len() > 0 {
        panic!("Parsing failed");
    }

    Ok(ast.document())
}

fn query_to_sql(root: AbstractQuery) -> Result<String> {
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

pub fn gql_to_sql(query: &str) -> Result<String> {
    // Parse GraphQL
    let document = parse_graphql_query(query).unwrap();

    // Convert GraphQL document to our own abstract query representation
    let root = AbstractQuery::new_from_document(document)?;

    // Convert to SQL query
    let sql = query_to_sql(root).unwrap();

    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::gql_to_sql;

    #[test]
    fn parser() {
        let query = "{
            festival_events_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b(document: \"0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543\") {
                document
                fields {
                    title
                    description
                }
            }
        }";

        let sql = gql_to_sql(query).unwrap();

        assert_eq!(
            sql,
            "SELECT \"title\", \"description\", \"document\" FROM \"festival_events_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b\" WHERE \"document\" = '0020b177ec1bf26dfb3b7010d473e6d44713b29b765b99c6e60ecbfae742de496543'"
        );
    }
}
