// SPDX-License-Identifier: AGPL-3.0-or-later

use apollo_parser::ast::{Argument as AstArgument, AstChildren, Definition, Document, Selection};
use apollo_parser::Parser;
use sea_query::{Alias, Expr, Query};

use crate::errors::Result;

#[derive(Debug)]
struct Root {
    /// Schema hash of the queried document view.
    schema: String,

    /// Filter or sorting arguments which can be applied to the results.
    arguments: Option<Vec<Argument>>,

    /// Fields which give general information on the given document view.
    meta_fields: Option<Vec<MetaField>>,

    /// Materialized field values of the document view.
    fields: Option<Vec<Field>>,
}

#[derive(Debug)]
enum Argument {
    /// Filter document views by document hash.
    DocumentHash(String),
}

#[derive(Debug)]
enum MetaField {
    /// Hash of each document view.
    DocumentHash,
}

#[derive(Debug)]
enum Field {
    /// Materialized value of document view.
    Value(String),

    /// Values of another related document view.
    Relation(Relation),
}

#[derive(Debug)]
struct Relation {
    /// Schema hash of the related document view.
    schema: String,

    /// Fields which give general information on the related document view.
    meta_fields: Option<Vec<MetaField>>,

    /// Materialized field values of the related document view.
    fields: Option<Vec<Field>>,
}

fn parse_graphql_query(query: &str) -> Result<Document> {
    let parser = Parser::new(query);
    let ast = parser.parse();

    if ast.errors().len() > 0 {
        panic!("Parsing failed");
    }

    let document = ast.document();

    Ok(document)
}

fn get_fields(selection_set: AstChildren<Selection>) -> Result<Vec<Field>> {
    let mut fields = Vec::<Field>::new();

    for selection in selection_set {
        match selection {
            Selection::Field(field) => {
                let name = field.name().expect("Needs a name").text().to_string();

                match field.selection_set() {
                    Some(set) => {
                        let (meta_fields, relation_fields) =
                            get_meta_fields(set.selections()).unwrap();

                        fields.push(Field::Relation(Relation {
                            schema: name,
                            meta_fields,
                            fields: relation_fields,
                        }));
                    }
                    None => {
                        fields.push(Field::Value(name));
                    }
                };
            }
            _ => panic!("Needs a field"),
        };
    }

    Ok(fields)
}

fn get_meta_fields(
    selection_set: AstChildren<Selection>,
) -> Result<(Option<Vec<MetaField>>, Option<Vec<Field>>)> {
    let mut meta = Vec::<MetaField>::new();
    let mut fields = None;

    for selection in selection_set {
        match selection {
            Selection::Field(field) => {
                let name = field.name().expect("Needs a name").text().to_string();

                match name.as_str() {
                    "fields" => {
                        let set = field.selection_set().expect("Needs selection set");
                        fields = Some(get_fields(set.selections()).unwrap());
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

fn get_root(document: Document) -> Result<Root> {
    let definition = document
        .definitions()
        .nth(0)
        .expect("Needs to contain at least one definition");

    if let Definition::OperationDefinition(op_def) = definition {
        let selection = op_def
            .selection_set()
            .expect("Needs to have a selection set")
            .selections()
            .nth(0)
            .expect("Needs to have one selection");

        if let Selection::Field(field) = selection {
            let schema = field
                .name()
                .expect("Needs to have a name")
                .text()
                .to_string();

            let arguments = match field.arguments() {
                Some(args) => Some(get_arguments(args.arguments()).unwrap()),
                None => None,
            };

            let (meta_fields, fields) = get_meta_fields(
                field
                    .selection_set()
                    .expect("Needs to have a selection set")
                    .selections(),
            )
            .unwrap();

            return Ok(Root {
                schema,
                meta_fields,
                arguments,
                fields,
            });
        } else {
            panic!("Needs to be a field");
        }
    } else {
        panic!("Needs to be an operation definition");
    }
}

fn get_arguments(args: AstChildren<AstArgument>) -> Result<Vec<Argument>> {
    let mut arguments = Vec::<Argument>::new();

    for arg in args {
        let name = arg.name().expect("Needs a name").to_string();
        let value = arg.value().expect("Needs a value").to_string();

        let arg_value = match name.as_str() {
            "document" => Argument::DocumentHash(value),
            _ => panic!("Unknown argument"),
        };

        arguments.push(arg_value);
    }

    Ok(arguments)
}

fn parse(query: &str) -> Result<Root> {
    let document = parse_graphql_query(query).unwrap();
    let root = get_root(document).unwrap();
    Ok(root)
}

fn validate(root: &Root, schema: u64) -> Result<()> {
    Ok(())
}

fn root_to_sql(root: Root) -> Result<String> {
    let mut query = Query::select();

    if root.fields.is_some() {
        for field in root.fields.unwrap() {
            if let Field::Value(name) = field {
                query.column(Alias::new(&name));
            } else {
                panic!("Relations not supported yet");
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

    if root.arguments.is_some() {
        for arg in root.arguments.unwrap() {
            match arg {
                Argument::DocumentHash(value) => {
                    query.and_where(Expr::col(Alias::new("document")).eq(value));
                }
            }
        }
    }

    let sql = query
        .from(Alias::new(&root.schema))
        .to_string(sea_query::PostgresQueryBuilder);

    Ok(sql)
}

pub fn gql_to_sql(query: &str) -> Result<String> {
    // @TODO: We will pass in the schema later ..
    let schema = 123;

    // Parse GraphQL to our own abstract query representation
    let root = parse(query).unwrap();

    // Validate query with application schema
    validate(&root, schema).unwrap();

    // Convert to SQL query
    let sql = root_to_sql(root).unwrap();

    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::gql_to_sql;

    #[test]
    fn parser() {
        let query = "{
            festivalEvents(document: ABC123) {
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
            "SELECT \"title\", \"description\", \"document\" FROM \"festivalEvents\" WHERE \"document\" = 'ABC123'"
        );
    }
}
