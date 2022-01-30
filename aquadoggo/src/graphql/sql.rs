// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryInto;

use apollo_parser::ast::{
    Argument as AstArgument, AstChildren, Definition, Document, Selection, Value,
};
use apollo_parser::Parser;
use p2panda_rs::hash::{Hash, HASH_SIZE};
use sea_query::{Alias, Expr, Query};

use crate::errors::Result;

const MAX_SCHEMA_NAME: usize = 64;
const SCHEMA_NAME_SEPARATOR: &str = "_";

fn parse_graphql_query(query: &str) -> Result<Document> {
    let parser = Parser::new(query);
    let ast = parser.parse();

    if ast.errors().len() > 0 {
        panic!("Parsing failed");
    }

    let document = ast.document();

    Ok(document)
}

type MetaFields = Option<Vec<MetaField>>;
type Fields = Option<Vec<Field>>;
type Arguments = Option<Vec<Argument>>;

#[derive(Debug)]
enum Argument {
    /// Filter document views by document hash.
    DocumentHash(Hash),
}

#[derive(Debug)]
enum MetaField {
    /// Hash of each document view.
    DocumentHash,
}

#[derive(Debug)]
enum Field {
    /// Name of the application data field to query.
    Name(String),

    /// Values of another related document view to query.
    Relation(Relation),
}

#[derive(Debug)]
struct Relation {
    /// Schema hash of the related document view.
    schema: Schema,

    /// Fields which give general information on the related document view.
    meta_fields: MetaFields,

    /// Materialized field values of the related document view.
    fields: Fields,
}

#[derive(Debug)]
struct Schema {
    /// Given name of this schema.
    name: String,

    /// Hash of the schema.
    hash: Hash,
}

impl Schema {
    pub fn parse(str: &str) -> Result<Self> {
        if str.len() > MAX_SCHEMA_NAME + SCHEMA_NAME_SEPARATOR.len() + (HASH_SIZE * 2) {
            panic!("Too long");
        }

        let mut separated = str.rsplitn(2, SCHEMA_NAME_SEPARATOR);

        let hash = separated
            .next()
            .expect("Could not parse hash")
            .to_string()
            .try_into()
            .expect("Invalid hash");

        let name = separated.next().expect("Could not parse name").to_string();

        Ok(Self { name, hash })
    }

    pub fn table_name(&self) -> String {
        format!(
            "{}{}{}",
            self.name,
            SCHEMA_NAME_SEPARATOR,
            self.hash.as_str()
        )
    }
}

#[derive(Debug)]
struct AbstractQuery {
    /// Schema hash of the queried document view.
    schema: Schema,

    /// Filter or sorting arguments which can be applied to the results.
    arguments: Arguments,

    /// Fields which give general information on the given document view.
    meta_fields: MetaFields,

    /// Materialized field values of the document view.
    fields: Fields,
}

impl AbstractQuery {
    fn get_fields(selection_set: AstChildren<Selection>) -> Result<Vec<Field>> {
        let mut fields = Vec::<Field>::new();

        for selection in selection_set {
            match selection {
                Selection::Field(field) => {
                    let field_name = field.name().expect("Needs a name").text().to_string();

                    match field.selection_set() {
                        Some(set) => {
                            let (meta_fields, relation_fields) =
                                Self::get_meta_fields(set.selections()).unwrap();

                            fields.push(Field::Relation(Relation {
                                schema: Schema::parse(&field_name).expect("Invalid schema"),
                                meta_fields,
                                fields: relation_fields,
                            }));
                        }
                        None => {
                            fields.push(Field::Name(field_name));
                        }
                    };
                }
                _ => panic!("Needs a field"),
            };
        }

        Ok(fields)
    }

    fn get_meta_fields(selection_set: AstChildren<Selection>) -> Result<(MetaFields, Fields)> {
        let mut meta = Vec::<MetaField>::new();
        let mut fields = None;

        for selection in selection_set {
            match selection {
                Selection::Field(field) => {
                    let name = field.name().expect("Needs a name").text().to_string();

                    match name.as_str() {
                        "fields" => {
                            let set = field.selection_set().expect("Needs selection set");
                            fields = Some(Self::get_fields(set.selections()).unwrap());
                        }
                        "document" => {
                            meta.push(MetaField::DocumentHash);
                        }
                        _ => panic!("Unknown meta field"),
                    };
                }
                _ => panic!("Needs a field"),
            };
        }

        let meta_option = if meta.is_empty() { None } else { Some(meta) };

        Ok((meta_option, fields))
    }

    fn get_arguments(args: AstChildren<AstArgument>) -> Result<Vec<Argument>> {
        let mut arguments = Vec::<Argument>::new();

        for arg in args {
            let name = arg.name().expect("Needs a name").to_string();
            let value = arg.value().expect("Needs a value");

            let arg_value = match name.as_str() {
                "document" => match value {
                    Value::StringValue(str_value) => {
                        let str: String = str_value.into();
                        let hash = str.try_into().expect("Invalid hash");
                        Argument::DocumentHash(hash)
                    }
                    _ => {
                        panic!("Expected string");
                    }
                },
                _ => panic!("Unknown argument"),
            };

            arguments.push(arg_value);
        }

        Ok(arguments)
    }

    pub fn new_from_query(query: &str) -> Result<Self> {
        let document = parse_graphql_query(query).unwrap();
        let root = Self::new_from_document(document).unwrap();
        Ok(root)
    }

    pub fn new_from_document(document: Document) -> Result<Self> {
        let definition = document
            .definitions()
            .next()
            .expect("Needs to contain at least one definition");

        if let Definition::OperationDefinition(op_def) = definition {
            let selection = op_def
                .selection_set()
                .expect("Needs to have a selection set")
                .selections()
                .next()
                .expect("Needs to have one selection");

            if let Selection::Field(field) = selection {
                let schema = field
                    .name()
                    .expect("Needs to have a name")
                    .text()
                    .to_string();

                let arguments = field
                    .arguments()
                    .map(|args| Self::get_arguments(args.arguments()).unwrap());

                let selections = field
                    .selection_set()
                    .expect("Needs to have a selection set")
                    .selections();

                let (meta_fields, fields) = Self::get_meta_fields(selections).unwrap();

                let schema = Schema::parse(&schema).expect("Invalid schema");

                Ok(Self {
                    schema,
                    meta_fields,
                    arguments,
                    fields,
                })
            } else {
                panic!("Needs to be a field");
            }
        } else {
            panic!("Needs to be an operation definition");
        }
    }
}

fn root_to_sql(root: AbstractQuery) -> Result<String> {
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
    // Parse GraphQL to our own abstract query representation
    let root = AbstractQuery::new_from_query(query).unwrap();

    // Convert to SQL query
    let sql = root_to_sql(root).unwrap();

    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::{gql_to_sql, Schema};

    #[test]
    fn parse_schema() {
        let schema = Schema::parse(
            "festivalevents_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b",
        )
        .unwrap();
        assert_eq!(schema.name, "festivalevents");
        assert_eq!(
            schema.hash.as_str(),
            "0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
        );

        let schema = Schema::parse(
            "mul_tiple_separators_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b",
        )
        .unwrap();
        assert_eq!(schema.name, "mul_tiple_separators");
        assert_eq!(
            schema.hash.as_str(),
            "0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
        );
    }

    #[test]
    fn invalid_schema() {
        // Invalid hash (wrong encoding)
        assert!(Schema::parse("test_thisisnotahash").is_err());

        // Separator missing
        assert!(Schema::parse(
            "withoutseparator0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b"
        )
        .is_err());

        // Invalid hash (too short)
        assert!(Schema::parse("invalid_hash_0020c6f23dbdc3b5c7b9ab46293111c48fc78b").is_err());

        // Schema name exceeds limit
        assert!(Schema::parse("too_long_name_with_many_characters_breaking_the_64_characters_limit_0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b").is_err());
    }

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
