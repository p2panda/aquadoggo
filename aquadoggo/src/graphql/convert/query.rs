// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryInto;

use apollo_parser::ast::{
    Argument as AstArgument, AstChildren, Definition, Document, Selection, Value,
};
use apollo_parser::Parser;
use p2panda_rs::hash::Hash;
use thiserror::Error;

use crate::graphql::convert::{Schema, SchemaParseError};

type MetaFields = Option<Vec<MetaField>>;
type Fields = Option<Vec<Field>>;
type Arguments = Option<Vec<Argument>>;

#[derive(Debug)]
pub enum Argument {
    /// Filter document views by document hash.
    DocumentHash(Hash),
}

#[derive(Debug)]
pub enum MetaField {
    /// Hash of each document view.
    DocumentHash,
}

#[derive(Debug)]
pub enum Field {
    /// Name of the application data field to query.
    Name(String),

    /// Values of another related document view to query.
    Relation(Relation),
}

#[derive(Debug)]
pub struct Relation {
    /// Schema hash of the related document view.
    pub schema: Schema,

    /// Fields which give general information on the related document view.
    pub meta_fields: MetaFields,

    /// Materialized field values of the related document view.
    pub fields: Fields,
}

#[derive(Error, Debug)]
#[allow(missing_copy_implementations)]
pub enum AbstractQueryError {
    #[error(transparent)]
    InvalidSchema(#[from] SchemaParseError),

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
pub struct AbstractQuery {
    /// Schema hash of the queried document view.
    pub schema: Schema,

    /// Filter or sorting arguments which can be applied to the results.
    pub arguments: Arguments,

    /// Fields which give general information on the given document view.
    pub meta_fields: MetaFields,

    /// Materialized field values of the document view.
    pub fields: Fields,
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
                        .text()
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
                        .text()
                        .to_string();

                    // No meta fields should have a selection set, except of "fields"
                    if field.selection_set().is_some() && name.as_str() != "fields" {
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
                .text()
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

    /// Validates and converts a GraphQL query into an `AbstractQuery` instance.
    ///
    /// Abstract queries represent a valid p2panda query to retreive document-view data from the
    /// database.
    pub fn new(query: &str) -> Result<Self, AbstractQueryError> {
        let parser = Parser::new(query);
        let ast = parser.parse();

        if ast.errors().len() > 0 {
            // @TODO: Handle parsing errors
            panic!("Parsing failed");
        }

        Self::new_from_document(ast.document())
    }

    /// Validates and converts a GraphQL document AST into an `AbstractQuery` instance.
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
                Err(AbstractQueryError::RootSelectionSetMissing)
            }
        } else {
            Err(AbstractQueryError::OperationMissing)
        }
    }
}
