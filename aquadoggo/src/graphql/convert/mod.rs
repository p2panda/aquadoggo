// SPDX-License-Identifier: AGPL-3.0-or-later

mod query;
mod schema;
mod sql;

pub use query::{AbstractQuery, AbstractQueryError, Argument, Field, MetaField};
pub use schema::{Schema, SchemaParseError};
pub use sql::query_to_sql;

pub fn gql_to_sql(query: &str) -> anyhow::Result<String> {
    // Convert GraphQL query to our own abstract query representation
    let root = AbstractQuery::new(query)?;

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
