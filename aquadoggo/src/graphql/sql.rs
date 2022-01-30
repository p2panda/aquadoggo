// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryInto;

use anyhow::Result;
use apollo_parser::ast::{
    Argument as AstArgument, AstChildren, Definition, Document, Selection, Value,
};
use apollo_parser::Parser;
use p2panda_rs::hash::{Hash, HASH_SIZE};
use sea_query::{Alias, Expr, Query};
use thiserror::Error;

const MAX_SCHEMA_NAME: usize = 64;
const SCHEMA_NAME_SEPARATOR: &str = "_";

fn parse_graphql_query(query: &str) -> Result<Document> {
    let parser = Parser::new(query);
    let ast = parser.parse();

    if ast.errors().len() > 0 {
        panic!("Parsing failed");
    }

    Ok(ast.document())
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

/// Parsed reference of a schema.
///
/// As per specification a schema is referenced by its name and hash in a query.
#[derive(Debug)]
struct Schema {
    /// Given name of this schema.
    name: String,

    /// Hash of the schema.
    hash: Hash,
}

#[derive(Error, Debug)]
#[allow(missing_copy_implementations)]
pub enum ParseSchemaError {
    #[error("Schema name is too long with {0} characters ({1} allowed)")]
    TooLong(usize, usize),

    #[error("Name and / or hash field is missing in string")]
    MissingFields,

    #[error(transparent)]
    InvalidHash(#[from] p2panda_rs::hash::HashError),
}

impl Schema {
    /// Parsing a schema string.
    pub fn parse(str: &str) -> Result<Self, ParseSchemaError> {
        // Allowed schema length is the maximum length of the schema name defined by the p2panda
        // specification, the length of the separator and hash size times two because of its
        // hexadecimal encoding.
        let max_len = MAX_SCHEMA_NAME + SCHEMA_NAME_SEPARATOR.len() + (HASH_SIZE * 2);
        if str.len() > max_len {
            return Err(ParseSchemaError::TooLong(str.len(), max_len));
        }

        // Split the string by the separator and return the iterator in reversed form
        let mut fields = str.rsplitn(2, SCHEMA_NAME_SEPARATOR);

        // Since we start parsing the string from the back, we look at the hash first
        let hash = fields
            .next()
            .ok_or(ParseSchemaError::MissingFields)?
            .try_into()?;

        // Finally parse the name of the given schema
        let name = fields
            .next()
            .ok_or(ParseSchemaError::MissingFields)?
            .to_string();

        Ok(Self { name, hash })
    }

    /// Returns name of SQL table holding document views of this schema.
    pub fn table_name(&self) -> String {
        format!(
            "{}{}{}",
            self.name,
            SCHEMA_NAME_SEPARATOR,
            self.hash.as_str()
        )
    }
}

#[derive(Error, Debug)]
#[allow(missing_copy_implementations)]
pub enum AbstractQueryError {
    #[error(transparent)]
    InvalidSchema(#[from] ParseSchemaError),

    #[error("Query needs to contain a root operation definition")]
    OperationMissing,

    #[error("Query definition needs to be in shorthand format")]
    OperationShorthandRequired,

    #[error("Selection set in root is invalid or missing")]
    RootSelectionSetMissing,

    #[error("Alias or directive types are not allowed in field")]
    FieldInvalid,

    #[error("Root field is missing a name")]
    FieldNameMissing,

    #[error("Root field is missing a selection set")]
    FieldSelectionSetMissing,

    #[error("Argument has invalid name or value")]
    ArgumentInvalid,

    #[error("Unknown argument used: {0}")]
    ArgumentUnknown(String),

    #[error("Invalid argument value used for: {0}")]
    ArgumentValueInvalid(String),

    #[error("Meta field is invalid")]
    MetaFieldInvalid,

    #[error("Meta field 'fields' needs a selection set")]
    MetaFieldNeedsSelectionSet,

    #[error("Meta field is not known: {0}")]
    MetaFieldUnknown(String),

    #[error("Application field is invalid")]
    ApplicationFieldInvalid,
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
    /// Convert fields to abstract query containing the names of the fields we want to query from
    /// the application data.
    ///
    /// We do not make sure yet if these fields actually exist in the application schema, this
    /// validation takes place after the query got converted.
    fn get_fields(selection_set: AstChildren<Selection>) -> Result<Vec<Field>, AbstractQueryError> {
        let mut fields = Vec::<Field>::new();

        for selection in selection_set {
            match selection {
                Selection::Field(field) => {
                    // Expect field to be simple
                    if field.directives().is_some()
                        || field.arguments().is_some()
                        || field.alias().is_some()
                    {
                        return Err(AbstractQueryError::ApplicationFieldInvalid);
                    }

                    // Expect field to have a name
                    let name = field
                        .name()
                        .ok_or(AbstractQueryError::ApplicationFieldInvalid)?
                        .to_string();

                    match field.selection_set() {
                        // Found a field with selection set which means we're querying a relation
                        // to another document-view
                        Some(set) => {
                            let (meta_fields, relation_fields) =
                                Self::get_meta_fields(set.selections())?;

                            // Expect a valid schema
                            let schema = Schema::parse(&name)?;

                            fields.push(Field::Relation(Relation {
                                schema,
                                meta_fields,
                                fields: relation_fields,
                            }));
                        }
                        // Found a regular application data field
                        None => {
                            fields.push(Field::Name(name));
                        }
                    };
                }
                _ => return Err(AbstractQueryError::ApplicationFieldInvalid),
            };
        }

        Ok(fields)
    }

    /// Convert fields of GraphQL document into abstract query.
    ///
    /// We call them "meta fields" since they do not relate to the application data fields but to
    /// general (meta) data we can ask about any document views.
    fn get_meta_fields(
        selection_set: AstChildren<Selection>,
    ) -> Result<(MetaFields, Fields), AbstractQueryError> {
        let mut meta = Vec::<MetaField>::new();
        let mut fields = None;

        for selection in selection_set {
            match selection {
                Selection::Field(field) => {
                    // Expect field to be simple
                    if field.directives().is_some()
                        || field.arguments().is_some()
                        || field.alias().is_some()
                    {
                        return Err(AbstractQueryError::MetaFieldInvalid);
                    }

                    // Expect field to have a name
                    let name = field
                        .name()
                        .ok_or(AbstractQueryError::MetaFieldInvalid)?
                        .to_string();

                    // No meta fields should have a selection set, except of "fields"
                    if field.selection_set().is_some() && name.as_str() == "fields" {
                        return Err(AbstractQueryError::MetaFieldInvalid);
                    }

                    match name.as_str() {
                        // "fields" is a special meta field which allows us to query application
                        // field data
                        "fields" => {
                            let selections = field
                                .selection_set()
                                .ok_or(AbstractQueryError::MetaFieldNeedsSelectionSet)?
                                .selections();
                            fields = Some(Self::get_fields(selections)?);
                        }
                        // Query the document hash for each result
                        "document" => {
                            meta.push(MetaField::DocumentHash);
                        }
                        _ => {
                            return Err(AbstractQueryError::MetaFieldUnknown(name));
                        }
                    };
                }
                _ => return Err(AbstractQueryError::MetaFieldInvalid),
            };
        }

        let meta_option = if meta.is_empty() { None } else { Some(meta) };
        Ok((meta_option, fields))
    }

    /// Convert field arguments of GraphQL document into abstract query arguments.
    ///
    /// Read more about GraphQL arguments here:
    /// https://spec.graphql.org/June2018/#sec-Language.Arguments
    fn get_arguments(args: AstChildren<AstArgument>) -> Result<Vec<Argument>, AbstractQueryError> {
        let mut arguments = Vec::<Argument>::new();

        for arg in args {
            // Expect a name and value for each argument
            let name = arg
                .name()
                .ok_or(AbstractQueryError::ArgumentInvalid)?
                .to_string();

            // Expect arguments of known name and value type
            let value = arg.value().ok_or(AbstractQueryError::ArgumentInvalid)?;

            let arg_value = match name.as_str() {
                // Filter results by "document" hash
                "document" => match value {
                    Value::StringValue(str_value) => {
                        let hash_str: String = str_value.into();
                        let hash = hash_str
                            .try_into()
                            .map_err(|_| AbstractQueryError::ArgumentValueInvalid(name))?;
                        Ok(Argument::DocumentHash(hash))
                    }
                    _ => Err(AbstractQueryError::ArgumentValueInvalid(name)),
                },
                _ => Err(AbstractQueryError::ArgumentUnknown(name)),
            }?;

            arguments.push(arg_value);
        }

        Ok(arguments)
    }

    /// Validates and converts an GraphQL document AST into an `AbstractQuery` instance which
    /// represents a valid p2panda query to retreive document view data from the database.
    ///
    /// GraphQL is a fairly expressive query language and we do not need / support all of its
    /// features for p2panda queries: Directives, Variables, Mutations, Subscriptions, Alias Types
    /// and so on are not allowed.
    ///
    /// The supported GraphQL query needs to be in shorthand format and can be roughly described
    /// like this:
    ///
    /// {
    ///     field_name(some_argument: "value") {
    ///         field
    ///         another_field
    ///         field_with_selection_set {
    ///             some_field
    ///         }
    ///     }
    /// }
    pub fn new_from_document(document: Document) -> Result<Self, AbstractQueryError> {
        // Expect an "OperationDefinition" in the root of this query
        let root_definition = document.definitions().next();

        if let Some(Definition::OperationDefinition(query_operation)) = root_definition {
            // Expect this "OperationDefinition" to be in shorthand format.
            //
            // "If a document contains only one query operation, and that query defines no
            // variables and contains no directives, that operation may be represented in a
            // short‐hand form which omits the query keyword and query name."
            //
            // See: https://spec.graphql.org/June2018/#sec-Language.Operations
            if query_operation.operation_type().is_some()
                || query_operation.variable_definitions().is_some()
                || query_operation.directives().is_some()
                || query_operation.name().is_some()
            {
                return Err(AbstractQueryError::OperationShorthandRequired);
            }

            // So far our query is empty in its shortform representation. As a first thing we
            // expect a root "SelectionSet" (aka "curly braces") around the whole query.
            //
            // See: https://spec.graphql.org/June2018/#sec-Selection-Sets
            let selection = query_operation
                .selection_set()
                .ok_or(AbstractQueryError::RootSelectionSetMissing)?
                .selections()
                .next();

            // We expect this root selection set to contain the actual query: A field with a name
            // (the schema), optional arguments (filter and sorting) and the to-be-queried fields
            // (meta data or application data).
            //
            // See: https://spec.graphql.org/June2018/#sec-Language.Fields
            if let Some(Selection::Field(field)) = selection {
                // Alias or directives are not supported
                if field.alias().is_some() || field.directives().is_some() {
                    return Err(AbstractQueryError::FieldInvalid);
                }

                // Expect a name describing the schema which should be queried
                let name = field
                    .name()
                    .ok_or(AbstractQueryError::FieldNameMissing)?
                    .text()
                    .to_string();
                let schema = Schema::parse(&name)?;

                // Check for optional arguments
                let arguments = match field.arguments() {
                    Some(args) => Some(Self::get_arguments(args.arguments())?),
                    None => None,
                };

                // Expect a selection set which holds the fields we want to query
                let selections = field
                    .selection_set()
                    .ok_or(AbstractQueryError::FieldSelectionSetMissing)?
                    .selections();
                let (meta_fields, fields) = Self::get_meta_fields(selections)?;

                Ok(Self {
                    schema,
                    meta_fields,
                    arguments,
                    fields,
                })
            } else {
                return Err(AbstractQueryError::RootSelectionSetMissing);
            }
        } else {
            return Err(AbstractQueryError::OperationMissing);
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
    // Parse GraphQL
    let document = parse_graphql_query(query).unwrap();

    // Convert GraphQL document to our own abstract query representation
    let root = AbstractQuery::new_from_document(document)?;

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
        // Missing name
        assert!(Schema::parse("_0020c6f23dbdc3b5c7b9ab46293111c48fc78b").is_err());

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
