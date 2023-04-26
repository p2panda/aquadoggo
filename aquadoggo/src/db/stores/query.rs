// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module offers a query API to find many p2panda documents, filtered or sorted by custom
//! parameters. The results are batched via cursor-based pagination.
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use anyhow::bail;
use p2panda_rs::document::traits::AsDocument;
use p2panda_rs::document::{DocumentId, DocumentViewFields, DocumentViewId};
use p2panda_rs::identity::PublicKey;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::{FieldName, Schema, SchemaId};
use p2panda_rs::storage_provider::error::DocumentStorageError;
use sqlx::any::AnyRow;
use sqlx::{query, query_as, FromRow};

use crate::db::models::utils::parse_document_view_field_rows;
use crate::db::models::{DocumentViewFieldRow, QueryRow};
use crate::db::query::{
    ApplicationFields, Cursor, Direction, Field, Filter, FilterBy, FilterSetting, LowerBound,
    MetaField, Order, Pagination, PaginationField, Select, UpperBound,
};
use crate::db::stores::OperationCursor;
use crate::db::types::StorageDocument;
use crate::db::SqlStore;

/// Configure query to select documents based on a relation list field.
pub struct RelationList {
    /// View id of document which holds relation list field.
    pub root: DocumentViewId,

    /// Field which contains the relation list values.
    pub field: FieldName,

    /// Type of relation list.
    pub list_type: RelationListType,
}

pub enum RelationListType {
    Pinned,
    Unpinned,
}

impl RelationList {
    pub fn new_unpinned(root: &DocumentViewId, field: &str) -> Self {
        Self {
            root: root.to_owned(),
            field: field.to_string(),
            list_type: RelationListType::Unpinned,
        }
    }

    pub fn new_pinned(root: &DocumentViewId, field: &str) -> Self {
        Self {
            root: root.to_owned(),
            field: field.to_string(),
            list_type: RelationListType::Pinned,
        }
    }
}

/// Cursor aiding pagination, represented as a base58-encoded string.
///
/// The encoding ensures that the cursor stays "opaque", API consumers to not read any further
/// semantic meaning into it, even though we keep some crucial information in it which help us
/// internally during pagination.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PaginationCursor {
    /// Unique identifier aiding us to determine the current row.
    pub operation_cursor: OperationCursor,

    /// Unique identifier aiding us to determine the row of the parent document's relation list.
    pub root_operation_cursor: Option<OperationCursor>,

    /// In relation list queries we use this field to represent the parent document holding that
    /// list.
    pub root_view_id: Option<DocumentViewId>,
}

impl PaginationCursor {
    pub fn new(
        operation_cursor: OperationCursor,
        root_operation_cursor: Option<OperationCursor>,
        root_view_id: Option<DocumentViewId>,
    ) -> Self {
        assert!(root_operation_cursor.is_some() == root_view_id.is_some());

        Self {
            operation_cursor,
            root_operation_cursor,
            root_view_id,
        }
    }
}

// This should _not_ be an underscore character since we're also parsing document view ids in the
// string which contain that character already
const CURSOR_SEPARATOR: char = '-';

impl Cursor for PaginationCursor {
    type Error = anyhow::Error;

    fn decode(encoded: &str) -> Result<Self, Self::Error> {
        let bytes = bs58::decode(encoded).into_vec()?;
        let decoded = std::str::from_utf8(&bytes)?;

        let parts: Vec<&str> = decoded.split(CURSOR_SEPARATOR).collect();
        match parts.len() {
            1 => Ok(Self::new(OperationCursor::from(parts[0]), None, None)),
            3 => Ok(Self::new(
                OperationCursor::from(parts[0]),
                Some(OperationCursor::from(parts[1])),
                Some(DocumentViewId::from_str(parts[2])?),
            )),
            _ => {
                bail!("Invalid amount of cursor parts");
            }
        }
    }

    fn encode(&self) -> String {
        bs58::encode(
            format!(
                "{}{}",
                self.operation_cursor,
                self.root_view_id
                    .as_ref()
                    .map_or("".to_string(), |view_id| format!(
                        "{}{}{}{}",
                        CURSOR_SEPARATOR,
                        self.root_operation_cursor
                            .as_ref()
                            .expect("Expect root_operation to be set when root_view_id is as well"),
                        CURSOR_SEPARATOR,
                        view_id
                    ))
            )
            .as_bytes(),
        )
        .into_string()
    }
}

#[derive(Default, Clone, Debug)]
pub struct PaginationData<C>
where
    C: Cursor,
{
    /// Number of all documents in queried collection.
    pub total_count: Option<u64>,

    /// Flag indicating if `endCursor` will return another page.
    pub has_next_page: bool,

    /// Flag indicating if `startCursor` will return another page.
    pub has_previous_page: bool,

    /// Cursor which can be used to paginate backwards.
    pub start_cursor: Option<C>,

    /// Cursor which can be used to paginate forwards.
    pub end_cursor: Option<C>,
}

pub type QueryResponse = (
    PaginationData<PaginationCursor>,
    Vec<(PaginationCursor, StorageDocument)>,
);

/// Query configuration to determine pagination cursor, selected fields, filters and order of
/// results.
#[derive(Debug, Clone)]
pub struct Query<C>
where
    C: Cursor,
{
    pub pagination: Pagination<C>,
    pub select: Select,
    pub filter: Filter,
    pub order: Order,
}

impl<C> Query<C>
where
    C: Cursor,
{
    pub fn new(
        pagination: &Pagination<C>,
        select: &Select,
        filter: &Filter,
        order: &Order,
    ) -> Self {
        Self {
            pagination: pagination.clone(),
            select: select.clone(),
            filter: filter.clone(),
            order: order.clone(),
        }
    }
}

/// Helper method to determine the field type of the given field by looking at the schema and
/// derive a SQL type cast function from it.
fn typecast_field_sql(
    sql_field: &str,
    field_name: &str,
    schema: &Schema,
    case_sensitive: bool,
) -> String {
    let field_type = schema
        .fields()
        .iter()
        .find_map(|(schema_field_name, field_type)| {
            if schema_field_name == field_name {
                Some(field_type)
            } else {
                None
            }
        })
        // We expect that at this stage we only deal with fields which really exist in the schema,
        // everything has been validated before
        .unwrap_or_else(|| panic!("Field '{}' not given in Schema", field_name));

    match field_type {
        p2panda_rs::schema::FieldType::Integer => {
            format!("CAST ({sql_field} AS INTEGER)")
        }
        p2panda_rs::schema::FieldType::Float => {
            format!("CAST ({sql_field} AS REAL)")
        }
        // All other types (booleans, relations, etc.) we keep as strings. We can not convert
        // booleans easily as they don't have their own datatype in SQLite
        _ => {
            if case_sensitive {
                sql_field.to_string()
            } else {
                format!("LOWER({sql_field})")
            }
        }
    }
}

/// Helper method to convert an operation value into the right representation in SQL.
fn value_sql(value: &OperationValue) -> String {
    match &value {
        // Note that we wrap boolean values into a string here, as our operation field values are
        // stored as strings in themselves and we can't typecast them to booleans due to a
        // limitation in SQLite (see typecast_field_sql method).
        //
        // When comparing meta fields like `is_edited` etc. do _not_ use this method since we're
        // dealing there with native boolean values instead of strings.
        OperationValue::Boolean(value) => (if *value { "'true'" } else { "'false'" }).to_string(),
        OperationValue::Integer(value) => value.to_string(),
        OperationValue::Float(value) => value.to_string(),
        OperationValue::String(value) => format!("'{value}'"),
        OperationValue::Relation(value) => {
            format!("'{}'", value.document_id())
        }
        OperationValue::RelationList(value) => value
            .iter()
            .map(|document_id| format!("'{document_id}'"))
            .collect::<Vec<String>>()
            .join(","),
        OperationValue::PinnedRelation(value) => format!("'{}'", value.view_id()),
        OperationValue::PinnedRelationList(value) => value
            .iter()
            .map(|view_id| format!("'{view_id}'"))
            .collect::<Vec<String>>()
            .join(","),
    }
}

/// Helper method to convert filter settings into SQL comparison operations.
fn cmp_sql(sql_field: &str, filter_setting: &FilterSetting) -> String {
    match &filter_setting.by {
        FilterBy::Element(value) => {
            if !filter_setting.exclusive {
                format!("{sql_field} = {}", value_sql(value))
            } else {
                format!("{sql_field} != {}", value_sql(value))
            }
        }
        FilterBy::Set(values_vec) => {
            let value_sql = values_vec
                .iter()
                .map(value_sql)
                .collect::<Vec<String>>()
                .join(",");

            if !filter_setting.exclusive {
                format!("{sql_field} IN ({})", value_sql)
            } else {
                format!("{sql_field} NOT IN ({})", value_sql)
            }
        }
        FilterBy::Interval(lower_value, upper_value) => {
            let mut values: Vec<String> = Vec::new();

            match lower_value {
                LowerBound::Unbounded => (),
                LowerBound::Greater(value) => {
                    values.push(format!("{sql_field} > {}", value_sql(value)));
                }
                LowerBound::GreaterEqual(value) => {
                    values.push(format!("{sql_field} >= {}", value_sql(value)));
                }
            }

            match upper_value {
                UpperBound::Unbounded => (),
                UpperBound::Lower(value) => {
                    values.push(format!("{sql_field} < {}", value_sql(value)));
                }
                UpperBound::LowerEqual(value) => {
                    values.push(format!("{sql_field} <= {}", value_sql(value)));
                }
            }

            values.join(" AND ")
        }
        FilterBy::Contains(OperationValue::String(value)) => {
            if !filter_setting.exclusive {
                format!("{sql_field} LIKE '%{value}%'")
            } else {
                format!("{sql_field} NOT LIKE '%{value}%'")
            }
        }
        _ => panic!("Unsupported filter"),
    }
}

/// Helper method to join optional SQL strings into one, separated by a comma.
fn concatenate_sql(items: &[Option<String>]) -> String {
    items
        .iter()
        .filter_map(|item| item.as_ref().cloned())
        .collect::<Vec<String>>()
        .join(", ")
}

fn where_filter_sql(filter: &Filter, schema: &Schema) -> String {
    filter
        .iter()
        .filter_map(|filter_setting| {
            match &filter_setting.field {
                Field::Meta(MetaField::Owner) => {
                    Some(format!("AND {}", cmp_sql("owner", filter_setting)))
                }
                Field::Meta(MetaField::Edited) => {
                    if let FilterBy::Element(OperationValue::Boolean(filter_value)) =
                        filter_setting.by
                    {
                        // Convert the boolean value manually here since we're dealing with native
                        // boolean values instead of operation field value strings
                        let edited_flag = if filter_value { "true" } else { "false" };
                        Some(format!("AND is_edited = {edited_flag}"))
                    } else {
                        None
                    }
                }
                Field::Meta(MetaField::Deleted) => {
                    if let FilterBy::Element(OperationValue::Boolean(filter_value)) =
                        filter_setting.by
                    {
                        // Convert the boolean value manually here since we're dealing with native
                        // boolean values instead of operation field value strings
                        let deleted_flag = if filter_value { "true" } else { "false" };
                        Some(format!("AND documents.is_deleted = {deleted_flag}"))
                    } else {
                        None
                    }
                }
                Field::Meta(MetaField::DocumentId) => Some(format!(
                    "AND {}",
                    cmp_sql("documents.document_id", filter_setting)
                )),
                Field::Meta(MetaField::DocumentViewId) => Some(format!(
                    "AND {}",
                    cmp_sql("documents.document_view_id", filter_setting)
                )),
                Field::Field(field_name) => {
                    let field_sql = typecast_field_sql("operation_fields_v1.value", field_name, schema, true);
                    let filter_cmp = cmp_sql(&field_sql, filter_setting);

                    Some(format!(
                        r#"
                        AND EXISTS (
                            SELECT
                                operation_fields_v1.value
                            FROM
                                operation_fields_v1
                            WHERE
                                operation_fields_v1.name = '{field_name}'
                                AND
                                    {filter_cmp}
                                AND
                                    operation_fields_v1.operation_id = document_view_fields.operation_id
                        )
                        "#
                    ))
                }
            }
        })
        .collect::<Vec<String>>()
        .join("\n")
}

fn where_pagination_sql(
    pagination: &Pagination<PaginationCursor>,
    fields: &ApplicationFields,
    list: Option<&RelationList>,
    schema: &Schema,
    order: &Order,
) -> String {
    // Ignore pagination if we're in a relation list query and the cursor does not match the parent
    // document view id
    if let (Some(relation_list), Some(cursor)) = (list, pagination.after.as_ref()) {
        if Some(&relation_list.root) != cursor.root_view_id.as_ref() {
            return "".to_string();
        }
    }

    if pagination.after.is_none() {
        return "".to_string();
    }

    // Unwrap as we know now that a cursor exists
    let cursor = pagination.after.as_ref().unwrap();
    let operation_cursor = &cursor.operation_cursor;

    let cursor_sql = match list {
        Some(_) => {
            let root_cursor = cursor
                .root_operation_cursor
                .as_ref()
                .expect("Expect root_operation_cursor to be set when querying relation list");

            format!("operation_fields_v1_list.cursor > '{root_cursor}'")
        }
        None => {
            format!("operation_fields_v1.cursor > '{operation_cursor}'")
        }
    };

    let cmp_direction = match order.direction {
        Direction::Ascending => ">",
        Direction::Descending => "<",
    };

    match &order.field {
        // If no ordering has been applied we can simply paginate over the unique cursor. If we're
        // in a relation list we need to paginate over the unique list index.
        None => match list {
            None => {
                format!("AND operation_fields_v1.cursor > '{operation_cursor}'")
            }
            Some(_) => {
                let root_cursor = cursor
                    .root_operation_cursor
                    .as_ref()
                    .expect("Expect root_operation_cursor to be set when querying relation list");

                let cmp_value = format!(
                    r#"
                    -- When ordering is activated we need to compare against the value
                    -- of the ordered field - but from the row where the cursor points at
                    SELECT
                        operation_fields_v1.list_index
                    FROM
                        operation_fields_v1
                    WHERE
                        operation_fields_v1.cursor = '{root_cursor}'
                    "#
                );

                // List indexes are always unique so we can simply just compare them like that
                format!("AND operation_fields_v1_list.list_index > ({cmp_value})")
            }
        },

        // Ordering over a meta field
        Some(Field::Meta(meta_field)) => {
            let (cmp_value, cmp_field) = match meta_field {
                MetaField::DocumentId => {
                    let cmp_value = format!(
                        r#"
                        SELECT
                            operations_v1.document_id
                        FROM
                            operation_fields_v1
                            JOIN operations_v1
                                On operation_fields_v1.operation_id = operations_v1.operation_id
                        WHERE
                            operation_fields_v1.cursor = '{operation_cursor}'
                        LIMIT 1
                        "#
                    );

                    (cmp_value, "documents.document_id")
                }
                MetaField::DocumentViewId => {
                    let cmp_value = format!(
                        r#"
                        SELECT
                            document_view_fields.document_view_id
                        FROM
                            operation_fields_v1
                            JOIN document_view_fields
                                On operation_fields_v1.operation_id = document_view_fields.operation_id
                        WHERE
                            operation_fields_v1.cursor = '{operation_cursor}'
                        LIMIT 1
                        "#
                    );

                    (cmp_value, "documents.document_view_id")
                }
                MetaField::Owner | MetaField::Edited | MetaField::Deleted => {
                    // @TODO: See issue: https://github.com/p2panda/aquadoggo/issues/326
                    todo!("Not implemented");
                }
            };

            if fields.is_empty() && list.is_none() {
                // When a root query was grouped by documents / no fields have been selected we can
                // assume that document id and view id are unique
                format!("AND {cmp_field} {cmp_direction} ({cmp_value})")
            } else {
                format!(
                    r#"
                    AND (
                        {cmp_field} {cmp_direction} ({cmp_value})
                        OR
                        (
                            {cmp_field} = ({cmp_value})
                            AND
                                {cursor_sql}
                        )
                    )
                    "#
                )
            }
        }

        // Ordering over an application field
        Some(Field::Field(order_field_name)) => {
            let cmp_field =
                typecast_field_sql("operation_fields_v1.value", order_field_name, schema, false);

            let cmp_value = format!(
                r#"
                SELECT
                    {cmp_field}
                FROM
                    operation_fields_v1
                WHERE
                    operation_fields_v1.cursor = '{operation_cursor}'
                "#
            );

            format!(
                r#"
                AND EXISTS (
                    SELECT
                        operation_fields_v1.value
                    FROM
                        operation_fields_v1
                    WHERE
                        operation_fields_v1.name = '{order_field_name}'
                        AND
                            operation_fields_v1.operation_id = document_view_fields.operation_id
                        AND
                            (
                                {cmp_field} {cmp_direction} ({cmp_value})
                                OR
                                (
                                    {cmp_field} = ({cmp_value})
                                    AND
                                        {cursor_sql}
                                )
                            )
                )
                "#
            )
        }
    }
}

fn group_sql(fields: &ApplicationFields) -> String {
    if fields.is_empty() {
        // Only one row per document when no field was selected
        "GROUP BY documents.document_id".to_string()
    } else {
        "".to_string()
    }
}

fn order_sql(order: &Order, schema: &Schema, list: Option<&RelationList>) -> String {
    // Create custom ordering if query set one
    let custom = order
        .field
        .as_ref()
        .map(|field| match field {
            Field::Meta(MetaField::DocumentId) => "documents.document_id".to_string(),
            Field::Meta(MetaField::DocumentViewId) => "documents.document_view_id".to_string(),
            Field::Meta(MetaField::Owner) => "owner".to_string(),
            Field::Meta(MetaField::Edited) => "is_edited".to_string(),
            Field::Meta(MetaField::Deleted) => "is_deleted".to_string(),
            Field::Field(field_name) => {
                format!(
                    r#"
                    (
                        SELECT
                            {}
                        FROM
                            operation_fields_v1
                            LEFT JOIN document_view_fields
                                ON operation_fields_v1.operation_id = document_view_fields.operation_id
                        WHERE
                            operation_fields_v1.name = '{}'
                            AND document_view_fields.document_view_id = documents.document_view_id
                        LIMIT 1
                    )
                    "#,
                    typecast_field_sql("operation_fields_v1.value", field_name, schema, false),
                    field_name,
                )
            }
        })
        .map(|field| {
            let direction = match order.direction {
                Direction::Ascending => "ASC",
                Direction::Descending => "DESC",
            };

            format!("{field} {direction}")
        });

    // .. and by relation list index, in case we're querying one
    let list_sql = match (&order.field, list) {
        (None, Some(_)) => Some("operation_fields_v1_list.list_index ASC".to_string()),
        _ => None,
    };

    // On top we sort always by the unique operation cursor in case the previous order value is
    // equal between two rows
    let cursor_sql = match list {
        Some(_) => Some("operation_fields_v1_list.cursor ASC".to_string()),
        None => Some("operation_fields_v1.cursor ASC".to_string()),
    };

    let order = concatenate_sql(&[custom, list_sql, cursor_sql]);

    format!("ORDER BY {order}")
}

fn limit_sql<C>(pagination: &Pagination<C>, fields: &ApplicationFields) -> (u64, String)
where
    C: Cursor,
{
    // We multiply the value by the number of fields we selected. If no fields have been selected
    // we just take the page size as is
    let page_size = pagination.first.get() * std::cmp::max(1, fields.len() as u64);

    // ... and add + 1 for the "has next page" flag
    (page_size, format!("LIMIT {page_size} + 1"))
}

fn where_fields_sql(fields: &ApplicationFields) -> String {
    if fields.is_empty() {
        "".to_string()
    } else {
        let fields_sql: Vec<String> = fields.iter().map(|field| format!("'{field}'")).collect();
        format!(
            "AND operation_fields_v1.name IN ({})",
            fields_sql.join(", ")
        )
    }
}

fn select_edited_sql(select: &Select) -> Option<String> {
    if select.fields.contains(&Field::Meta(MetaField::Edited)) {
        let sql = r#"
        -- Check if there is more operations next to the initial "create" operation
        (
            SELECT
                operations_v1.public_key
            FROM
                operations_v1
            WHERE
                operations_v1.action != "create"
                AND
                    operations_v1.document_id = documents.document_id
            LIMIT 1
        ) AS is_edited
        "#
        .to_string();

        Some(sql)
    } else {
        None
    }
}

fn select_owner_sql(select: &Select) -> Option<String> {
    if select.fields.contains(&Field::Meta(MetaField::Owner)) {
        // The original owner of a document we get by checking which public key signed the
        // "create" operation, the hash of that operation is the same as the document id
        let sql = r#"
        (
            SELECT
                operations_v1.public_key
            FROM
                operations_v1
            WHERE
                operations_v1.operation_id = documents.document_id
        ) AS owner
        "#
        .to_string();

        Some(sql)
    } else {
        None
    }
}

fn select_fields_sql(fields: &ApplicationFields) -> Vec<Option<String>> {
    let mut select = Vec::new();

    if !fields.is_empty() {
        // We get the application data by selecting the name, value and type
        select.push(Some("operation_fields_v1.name".to_string()));
        select.push(Some("operation_fields_v1.value".to_string()));
        select.push(Some("operation_fields_v1.field_type".to_string()));
    }

    select
}

fn select_cursor_sql(list: Option<&RelationList>) -> Vec<Option<String>> {
    match list {
        Some(_) => {
            vec![
                Some("operation_fields_v1.cursor AS cmp_value_cursor".to_string()),
                Some("operation_fields_v1_list.cursor AS root_cursor".to_string()),
            ]
        }
        None => vec![Some(
            "operation_fields_v1.cursor AS cmp_value_cursor".to_string(),
        )],
    }
}

fn where_sql(schema: &Schema, fields: &ApplicationFields, list: Option<&RelationList>) -> String {
    let schema_id = schema.id();

    // Only one row per field: restrict relation lists to first list item
    let list_index_sql = "AND operation_fields_v1.list_index = 0";

    // Always select at least one field, even if user didn't select one. If we wouldn't select a
    // field we would potentially receive multiple rows per document even though when we're only
    // interested in one per row.
    let extra_field_select = if fields.is_empty() {
        let (field_name, _) = schema.fields().iter().next().unwrap();
        format!("AND operation_fields_v1.name = '{field_name}'")
    } else {
        "".to_string()
    };

    match list {
        None => {
            // Filter by the queried schema of that collection
            format!(
                r#"
                documents.schema_id = '{schema_id}'
                {list_index_sql}
                {extra_field_select}
                "#
            )
        }
        Some(relation_list) => {
            // Filter by the parent document view id of this relation list
            let field_name = &relation_list.field;
            let view_id = &relation_list.root;
            let field_type = match relation_list.list_type {
                RelationListType::Pinned => "pinned_relation_list",
                RelationListType::Unpinned => "relation_list",
            };

            format!(
                r#"
                document_view_fields_list.document_view_id = '{view_id}'
                AND
                    operation_fields_v1_list.field_type = '{field_type}'
                AND
                    operation_fields_v1_list.name = '{field_name}'
                {list_index_sql}
                {extra_field_select}
                "#
            )
        }
    }
}

fn from_sql(list: Option<&RelationList>) -> String {
    match list {
        Some(relation_list) => {
            let filter_sql = match relation_list.list_type {
                RelationListType::Pinned => "documents.document_view_id",
                RelationListType::Unpinned => "documents.document_id",
            };

            format!(
                r#"
                -- Select relation list of parent document first ..
                document_view_fields document_view_fields_list
                JOIN operation_fields_v1 operation_fields_v1_list
                    ON
                        document_view_fields_list.operation_id = operation_fields_v1_list.operation_id
                    AND
                        document_view_fields_list.name = operation_fields_v1_list.name

                -- .. and join the related documents afterwards
                JOIN documents
                    ON
                        operation_fields_v1_list.value = {filter_sql}
                JOIN document_view_fields
                    ON documents.document_view_id = document_view_fields.document_view_id
                "#
            )
        }
        // Otherwise just query the documents directly
        None => r#"
            documents
            JOIN document_view_fields
                ON documents.document_view_id = document_view_fields.document_view_id
        "#
        .to_string(),
    }
}

impl SqlStore {
    /// Returns a paginated collection of documents from the database which can be filtered and
    /// ordered by custom parameters.
    ///
    /// When passing a `list` configuration the query will run against the documents of a (pinned
    /// and unpinned) relation list instead.
    pub async fn query(
        &self,
        schema: &Schema,
        args: &Query<PaginationCursor>,
        list: Option<&RelationList>,
    ) -> Result<QueryResponse, DocumentStorageError> {
        let schema_id = schema.id();

        // Get all selected application fields from query
        let application_fields = args.select.application_fields();

        // Generate SQL based on the given schema and query
        let mut select_vec = vec![
            // We get all the meta informations first, let's start with the document id, document
            // view id and id of the operation which holds the data
            Some("documents.document_id".to_string()),
            Some("documents.document_view_id".to_string()),
            Some("document_view_fields.operation_id".to_string()),
            // The deletion status of a document we already store in the database, let's select it
            // in any case since we get it for free
            Some("documents.is_deleted".to_string()),
        ];

        // .. also the unique cursor is very important, it will help us with cursor-based
        // pagination, especially over relation lists with duplicate documents
        select_vec.append(&mut select_cursor_sql(list));

        // All other fields we optionally select depending on the query
        select_vec.push(select_edited_sql(&args.select));
        select_vec.push(select_owner_sql(&args.select));
        select_vec.append(&mut select_fields_sql(&application_fields));

        let select = concatenate_sql(&select_vec);

        let from = from_sql(list);

        let where_ = where_sql(schema, &application_fields, list);
        let and_fields = where_fields_sql(&application_fields);
        let and_filters = where_filter_sql(&args.filter, schema);
        let and_pagination = where_pagination_sql(
            &args.pagination,
            &application_fields,
            list,
            schema,
            &args.order,
        );

        let group = group_sql(&application_fields);
        let order = order_sql(&args.order, schema, list);
        let (page_size, limit) = limit_sql(&args.pagination, &application_fields);

        let sea_quel = format!(
            r#"
            -- ░░░░░▄▀▀▀▄░░░░░░░░░░░░░░░░░
            -- ▄███▀░◐░░░▌░░░░░░░░░░░░░░░░
            -- ░░░░▌░░░░░▐░░░░░░░░░░░░░░░░
            -- ░░░░▐░░░░░▐░░░░░░░░░░░░░░░░
            -- ░░░░▌░░░░░▐▄▄░░░░░░░░░░░░░░
            -- ░░░░▌░░░░▄▀▒▒▀▀▀▀▄░░░░░░░░░
            -- ░░░▐░░░░▐▒▒▒▒▒▒▒▒▀▀▄░░░░░░░
            -- ░░░▐░░░░▐▄▒▒▒▒▒▒▒▒▒▒▀▄░░░░░
            -- ░░░░▀▄░░░░▀▄▒▒▒▒▒▒▒▒▒▒▀▄░░░
            -- ░░░░░░▀▄▄▄▄▄█▄▄▄▄▄▄▄▄▄▄▄▀▄░
            -- ░░░░░░░░░░░▌▌░▌▌░░░░░░░░░░░
            -- ░░░░░░░░░░░▌▌░▌▌░░░░░░░░░░░
            -- ░░░░░░░░░▄▄▌▌▄▌▌░░░░░░░░░░░
            --
            -- Hello, my name is sea gull.
            -- I designed a SQL query which
            -- is as smart, as big and as
            -- annoying as myself.

            SELECT
                {select}

            FROM
                -- Usually we query the "documents" table first. In case we're looking at a relation
                -- list this is slighly more complicated and we need to do some additional JOINs
                {from}

                -- We need to add some more JOINs to get the values from the operations
                JOIN operation_fields_v1
                    ON
                        document_view_fields.operation_id = operation_fields_v1.operation_id
                        AND
                            document_view_fields.name = operation_fields_v1.name

            WHERE
                -- We filter by the queried schema of that collection, if it is a relation
                -- list we filter by the view id of the parent document
                {where_}

                -- .. and select only the operation fields we're interested in
                {and_fields}

                -- .. and further filter the data by custom parameters
                {and_filters}

                -- Lastly we batch all results into smaller chunks via cursor pagination
                {and_pagination}

            -- We always order the rows by document id and list index, but there might also be
            -- user-defined ordering on top of that
            {order}

            -- Connected to cursor pagination we limit the number of rows
            {limit}
        "#
        );

        let mut rows: Vec<QueryRow> = query_as::<_, QueryRow>(&sea_quel)
            .fetch_all(&self.pool)
            .await
            .map_err(|err| DocumentStorageError::FatalStorageError(err.to_string()))?;

        // We always query one more row than needed to find out if there's more data. This
        // information aids the user during pagination
        let has_next_page = if rows.len() as u64 > page_size {
            // Remove that last row from final results if it exists
            rows.pop();
            true
        } else {
            false
        };

        // Calculate the total number of (filtered) documents in this query
        let total_count = if args
            .pagination
            .fields
            .contains(&PaginationField::TotalCount)
        {
            Some(self.count(schema, args, list).await?)
        } else {
            None
        };

        // Finally convert everything into the right format
        let documents = convert_rows(rows, list, &application_fields, schema.id());

        // Determine cursors for pagination by looking at beginning and end of results
        let start_cursor = if args
            .pagination
            .fields
            .contains(&PaginationField::StartCursor)
        {
            documents.first().map(|(cursor, _)| cursor.to_owned())
        } else {
            None
        };

        let end_cursor = if args.pagination.fields.contains(&PaginationField::EndCursor) {
            documents.last().map(|(cursor, _)| cursor.to_owned())
        } else {
            None
        };

        let pagination_data = PaginationData {
            total_count,
            has_next_page,
            // @TODO: Implement backwards pagination, see related issue:
            // https://github.com/p2panda/aquadoggo/issues/325
            has_previous_page: false,
            start_cursor,
            end_cursor,
        };

        Ok((pagination_data, documents))
    }

    /// Query number of documents in filtered collection.
    pub async fn count(
        &self,
        schema: &Schema,
        args: &Query<PaginationCursor>,
        list: Option<&RelationList>,
    ) -> Result<u64, DocumentStorageError> {
        let application_fields = args.select.application_fields();

        let from = from_sql(list);
        let where_ = where_sql(schema, &application_fields, list);
        let and_filters = where_filter_sql(&args.filter, schema);

        let count_sql = format!(
            r#"
            SELECT
                COUNT(documents.document_id)

            FROM
                {from}

                JOIN operation_fields_v1
                    ON
                        document_view_fields.operation_id = operation_fields_v1.operation_id
                        AND
                            document_view_fields.name = operation_fields_v1.name

            WHERE
                {where_}
                {and_filters}

            -- Group application fields by name to make sure we get actual number of documents
            GROUP BY operation_fields_v1.name
            "#
        );

        let result: Option<(i64,)> = query_as(&count_sql)
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| DocumentStorageError::FatalStorageError(err.to_string()))?;

        let count = match result {
            Some(result) => result.0 as u64,
            None => 0,
        };

        Ok(count)
    }
}

/// Merges all operation fields from the database into documents.
///
/// Due to the special table layout we receive one row per operation field in the query. Usually we
/// need to merge multiple rows / operation fields fields into one document.
fn convert_rows(
    rows: Vec<QueryRow>,
    list: Option<&RelationList>,
    fields: &ApplicationFields,
    schema_id: &SchemaId,
) -> Vec<(PaginationCursor, StorageDocument)> {
    let mut converted: Vec<(PaginationCursor, StorageDocument)> = Vec::new();

    println!("=== ALL QUERY ROWS RETURNED FROM THE DATABASE ===");
    println!("{rows:#?}");

    if rows.is_empty() {
        return converted;
    }

    // Helper method to convert database row into final document and cursor type
    let finalize_document = |row: &QueryRow,
                             collected_fields: Vec<DocumentViewFieldRow>|
     -> (PaginationCursor, StorageDocument) {
        let fields = parse_document_view_field_rows(collected_fields);

        let document = StorageDocument {
            id: row.document_id.parse().unwrap(),
            fields: Some(fields),
            schema_id: schema_id.clone(),
            view_id: row.document_view_id.parse().unwrap(),
            author: (&row.owner).into(),
            deleted: row.is_deleted,
        };

        let cursor = row_to_cursor(row, list);

        (cursor, document)
    };

    let last_row = rows.last().unwrap().clone();

    let mut current = rows[0].clone();
    let mut current_fields = Vec::new();

    let rows_per_document = std::cmp::max(fields.len(), 1);

    println!("=== REDUCE ROWS INTO DOCUMENTS ===");
    for (index, row) in rows.into_iter().enumerate() {
        // We observed a new document coming up in the next row, time to change
        if index % rows_per_document == 0 && index > 0 {
            // Finalize the current document, convert it and push it into the final array
            let (cursor, document) = finalize_document(&current, current_fields);
            println!("Document at row index {index}: {}", document.id());
            println!("Fields: {:?}", document.fields().unwrap().keys());
            converted.push((cursor, document));

            // Change the pointer to the next document
            current = row.clone();
            current_fields = Vec::new();
        }

        // Collect every field of the document
        current_fields.push(DocumentViewFieldRow {
            document_id: row.document_id,
            document_view_id: row.document_view_id,
            operation_id: row.operation_id,
            name: row.name,
            list_index: 0,
            field_type: row.field_type,
            value: row.value,
        });
    }

    // Do it one last time at the end for the last document
    let (cursor, document) = finalize_document(&last_row, current_fields);
    println!("Final document: {}", document.id());
    println!("Fields: {:?}", document.fields().unwrap().keys());

    converted.push((cursor, document));

    converted
}

/// Get a cursor from a document row.
fn row_to_cursor(row: &QueryRow, list: Option<&RelationList>) -> PaginationCursor {
    match list {
        Some(relation_list) => {
            // If we're querying a relation list then we mention the document view id
            // of the parent document. This helps us later to understand _which_ of the
            // potentially many relation lists we want to paginate
            PaginationCursor::new(
                row.cmp_value_cursor.as_str().into(),
                Some(row.root_cursor.as_str().into()),
                Some(relation_list.root.clone()),
            )
        }
        None => PaginationCursor::new(row.cmp_value_cursor.as_str().into(), None, None),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use p2panda_rs::document::traits::AsDocument;
    use p2panda_rs::document::{DocumentViewFields, DocumentViewId};
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::{
        OperationFields, OperationId, OperationValue, PinnedRelationList, Relation,
    };
    use p2panda_rs::schema::{FieldType, Schema, SchemaId};
    use p2panda_rs::test_utils::fixtures::{key_pair, random_hash, schema_id};
    use rstest::rstest;
    use sqlx::query_as;

    use crate::db::models::{OptionalOwner, QueryRow};
    use crate::db::query::{
        Direction, Field, Filter, MetaField, Order, Pagination, PaginationField, Select,
    };
    use crate::db::stores::{OperationCursor, RelationList};
    use crate::db::types::StorageDocument;
    use crate::graphql::input_values::OrderDirection;
    use crate::test_utils::{
        add_document, add_schema, add_schema_and_documents, test_runner, TestNode,
    };

    use super::{convert_rows, PaginationCursor, Query};

    fn get_document_value(document: &StorageDocument, field: &str) -> OperationValue {
        document
            .fields()
            .expect("Expected document fields")
            .get(field)
            .expect(&format!("Expected '{field}' field to exist in document"))
            .value()
            .to_owned()
    }

    async fn create_events_test_data(
        node: &mut TestNode,
        key_pair: &KeyPair,
    ) -> (Schema, Vec<DocumentViewId>) {
        add_schema_and_documents(
            node,
            "events",
            vec![
                vec![
                    (
                        "title",
                        "Kids Bits! Chiptune for baby squirrels".into(),
                        None,
                    ),
                    ("date", "2023-04-17".into(), None),
                    ("ticket_price", 5.75.into(), None),
                ],
                vec![
                    ("title", "The Pandadoodle Flute Trio".into(), None),
                    ("date", "2023-04-14".into(), None),
                    ("ticket_price", 12.5.into(), None),
                ],
                vec![
                    ("title", "Eventual Consistent Grapefruit".into(), None),
                    ("date", "2023-05-02".into(), None),
                    ("ticket_price", 24.99.into(), None),
                ],
                vec![
                    (
                        "title",
                        "Bamboo-Scrumble Rumba Night - Xmas special".into(),
                        None,
                    ),
                    ("date", "2023-12-20".into(), None),
                    ("ticket_price", 99.0.into(), None),
                ],
                vec![
                    ("title", "Shoebill - Non-migratory Shoegaze".into(), None),
                    ("date", "2023-09-09".into(), None),
                    ("ticket_price", 10.00.into(), None),
                ],
            ],
            key_pair,
        )
        .await
    }

    async fn create_venues_test_data(
        node: &mut TestNode,
        key_pair: &KeyPair,
    ) -> (Schema, Vec<DocumentViewId>) {
        add_schema_and_documents(
            node,
            "venues",
            vec![
                vec![("name", "World Wide Feld".into(), None)],
                vec![("name", "Internet Explorer".into(), None)],
                vec![("name", "p4p space".into(), None)],
            ],
            key_pair,
        )
        .await
    }

    async fn create_visited_test_data(
        node: &mut TestNode,
        venues_view_ids: Vec<DocumentViewId>,
        venues_schema: Schema,
        key_pair: &KeyPair,
    ) -> (Schema, Vec<DocumentViewId>) {
        add_schema_and_documents(
            node,
            "visited",
            vec![
                vec![
                    ("user", "seagull".into(), None),
                    (
                        "venues",
                        vec![
                            venues_view_ids[0].clone(),
                            venues_view_ids[0].clone(),
                            venues_view_ids[1].clone(),
                            venues_view_ids[2].clone(),
                            venues_view_ids[0].clone(),
                            venues_view_ids[0].clone(),
                            venues_view_ids[1].clone(),
                        ]
                        .into(),
                        Some(venues_schema.id().to_owned()),
                    ),
                ],
                vec![
                    ("user", "panda".into(), None),
                    (
                        "venues",
                        vec![
                            venues_view_ids[1].clone(),
                            venues_view_ids[1].clone(),
                            venues_view_ids[2].clone(),
                        ]
                        .into(),
                        Some(venues_schema.id().to_owned()),
                    ),
                ],
            ],
            &key_pair,
        )
        .await
    }

    #[rstest]
    #[case::order_by_date_asc(
        Query::new(
            &Pagination::default(),
            &Select::new(&["title".into()]),
            &Filter::new(),
            &Order::new(&"date".into(), &Direction::Ascending),
        ),
        "title".into(),
        vec![
            "The Pandadoodle Flute Trio".into(),
            "Kids Bits! Chiptune for baby squirrels".into(),
            "Eventual Consistent Grapefruit".into(),
            "Shoebill - Non-migratory Shoegaze".into(),
            "Bamboo-Scrumble Rumba Night - Xmas special".into(),
        ],
    )]
    #[case::order_by_date_desc(
        Query::new(
            &Pagination::default(),
            &Select::new(&["date".into()]),
            &Filter::new(),
            &Order::new(&"date".into(), &Direction::Descending),
        ),
        "date".into(),
        vec![
            "2023-12-20".into(),
            "2023-09-09".into(),
            "2023-05-02".into(),
            "2023-04-17".into(),
            "2023-04-14".into(),
        ],
    )]
    #[case::filter_by_ticket_price_range(
        Query::new(
            &Pagination::default(),
            &Select::new(&["ticket_price".into()]),
            &Filter::new().fields(&[
                ("ticket_price_gt", &[10.0.into()]),
                ("ticket_price_lt", &[50.0.into()]),
            ]),
            &Order::new(&"ticket_price".into(), &Direction::Ascending),
        ),
        "ticket_price".into(),
        vec![
            12.5.into(),
            24.99.into(),
        ],
    )]
    #[case::filter_by_search_string(
        Query::new(
            &Pagination::default(),
            &Select::new(&["title".into()]),
            &Filter::new().fields(&[
                ("title_contains", &["baby".into()]),
            ]),
            &Order::default(),
        ),
        "title".into(),
        vec![
            "Kids Bits! Chiptune for baby squirrels".into(),
        ],
    )]
    fn basic_queries(
        key_pair: KeyPair,
        #[case] args: Query<PaginationCursor>,
        #[case] selected_field: String,
        #[case] expected_fields: Vec<OperationValue>,
    ) {
        test_runner(|mut node: TestNode| async move {
            let (schema, view_ids) = create_events_test_data(&mut node, &key_pair).await;

            let (_pagination_data, documents) = node
                .context
                .store
                .query(&schema, &args, None)
                .await
                .expect("Query failed");

            assert_eq!(documents.len(), expected_fields.len());

            // Compare expected field values over all returned documents
            for (index, expected_value) in expected_fields.into_iter().enumerate() {
                assert_eq!(
                    get_document_value(&documents[index].1, &selected_field),
                    expected_value
                );
            }
        });
    }

    #[rstest]
    fn pagination_over_ordered_view_ids(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let (schema, mut view_ids) = create_events_test_data(&mut node, &key_pair).await;
            let view_ids_len = view_ids.len();

            // Sort created documents by document view id, to compare to similarily sorted query
            // results
            view_ids.sort();

            let mut cursor: Option<PaginationCursor> = None;

            let mut args = Query::new(
                &Pagination::new(
                    &NonZeroU64::new(1).unwrap(),
                    cursor.as_ref(),
                    &vec![
                        PaginationField::TotalCount,
                        PaginationField::EndCursor,
                        PaginationField::HasNextPage,
                    ],
                ),
                &Select::new(&[Field::Meta(MetaField::DocumentViewId)]),
                &Filter::default(),
                &Order::new(
                    &Field::Meta(MetaField::DocumentViewId),
                    &Direction::Ascending,
                ),
            );

            // Go through all pages, one document at a time
            for (index, view_id) in view_ids.into_iter().enumerate() {
                args.pagination.after = cursor;

                let (pagination_data, documents) = node
                    .context
                    .store
                    .query(&schema, &args, None)
                    .await
                    .expect("Query failed");

                match pagination_data.end_cursor {
                    Some(end_cursor) => {
                        cursor = Some(end_cursor);
                    }
                    None => panic!("Expected cursor"),
                }

                if view_ids_len - 1 == index {
                    assert_eq!(pagination_data.has_next_page, false);
                } else {
                    assert_eq!(pagination_data.has_next_page, true);
                }

                assert_eq!(pagination_data.total_count, Some(5));
                assert_eq!(documents.len(), 1);
                assert_eq!(documents[0].1.view_id, view_id);
                assert_eq!(cursor.as_ref(), Some(&documents[0].0));
            }

            // Query one last time after we paginated through everything
            args.pagination.after = cursor;

            let (pagination_data, documents) = node
                .context
                .store
                .query(&schema, &args, None)
                .await
                .expect("Query failed");

            assert_eq!(pagination_data.total_count, Some(5));
            assert_eq!(pagination_data.end_cursor, None);
            assert_eq!(pagination_data.has_next_page, false);
            assert_eq!(documents.len(), 0);
        });
    }

    #[rstest]
    fn pinned_relation_list(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let (venues_schema, mut venues_view_ids) =
                create_venues_test_data(&mut node, &key_pair).await;

            let (visited_schema, mut visited_view_ids) = create_visited_test_data(
                &mut node,
                venues_view_ids,
                venues_schema.to_owned(),
                &key_pair,
            )
            .await;

            let args = Query::new(
                &Pagination::new(
                    &NonZeroU64::new(10).unwrap(),
                    None,
                    &vec![
                        PaginationField::TotalCount,
                        PaginationField::EndCursor,
                        PaginationField::HasNextPage,
                    ],
                ),
                &Select::new(&["name".into()]),
                &Filter::default(),
                &Order::default(),
            );

            // Select the pinned relation list "venues" of the first visited document
            let list = RelationList::new_pinned(&visited_view_ids[0], "venues".into());

            let (pagination_data, documents) = node
                .context
                .store
                .query(&venues_schema, &args, Some(&list))
                .await
                .expect("Query failed");

            assert_eq!(documents.len(), 7);
            assert_eq!(pagination_data.total_count, Some(7));
            assert_eq!(
                get_document_value(&documents[0].1, "name"),
                OperationValue::String("World Wide Feld".to_string())
            );
            assert_eq!(
                get_document_value(&documents[4].1, "name"),
                OperationValue::String("World Wide Feld".to_string())
            );

            // Select the pinned relation list "venues" of the second visited document
            let list = RelationList::new_pinned(&visited_view_ids[1], "venues".into());

            let (pagination_data, documents) = node
                .context
                .store
                .query(&venues_schema, &args, Some(&list))
                .await
                .expect("Query failed");

            assert_eq!(documents.len(), 3);
            assert_eq!(pagination_data.total_count, Some(3));
            assert_eq!(
                get_document_value(&documents[0].1, "name"),
                OperationValue::String("Internet Explorer".to_string())
            );
            assert_eq!(
                get_document_value(&documents[1].1, "name"),
                OperationValue::String("Internet Explorer".to_string())
            );
            assert_eq!(
                get_document_value(&documents[2].1, "name"),
                OperationValue::String("p4p space".to_string())
            );
        });
    }

    #[rstest]
    fn empty_pinned_relation_list(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let (venues_schema, mut venues_view_ids) =
                create_venues_test_data(&mut node, &key_pair).await;

            let visited_schema = add_schema(
                &mut node,
                "visited",
                vec![(
                    "venues",
                    FieldType::PinnedRelationList(venues_schema.id().clone()),
                )],
                &key_pair,
            )
            .await;

            let visited_view_id = add_document(
                &mut node,
                visited_schema.id(),
                vec![(
                    "venues",
                    OperationValue::PinnedRelationList(PinnedRelationList::new(vec![])),
                )],
                &key_pair,
            )
            .await;

            // Query selecting only meta field.
            let args = Query::new(
                &Pagination::new(
                    &NonZeroU64::new(10).unwrap(),
                    None,
                    &vec![
                        PaginationField::TotalCount,
                        PaginationField::EndCursor,
                        PaginationField::HasNextPage,
                    ],
                ),
                &Select::new(&[Field::Meta(MetaField::DocumentId)]),
                &Filter::default(),
                &Order::default(),
            );

            // Select the pinned relation list "venues" for the visited document
            let list = RelationList::new_pinned(&visited_view_id, "venues".into());

            let (pagination_data, documents) = node
                .context
                .store
                .query(&venues_schema, &args, Some(&list))
                .await
                .expect("Query failed");

            assert!(documents.is_empty());

            // Query selecting application field.
            let args = Query::new(
                &Pagination::new(
                    &NonZeroU64::new(10).unwrap(),
                    None,
                    &vec![
                        PaginationField::TotalCount,
                        PaginationField::EndCursor,
                        PaginationField::HasNextPage,
                    ],
                ),
                &Select::new(&["name".into()]),
                &Filter::default(),
                &Order::default(),
            );

            // Select the pinned relation list "venues" for the visited document
            let list = RelationList::new_pinned(&visited_view_id, "venues".into());

            let (pagination_data, documents) = node
                .context
                .store
                .query(&venues_schema, &args, Some(&list))
                .await
                .expect("Query failed");

            assert!(documents.is_empty());

            // Query selecting application field.
            let args = Query::new(
                &Pagination::new(
                    &NonZeroU64::new(10).unwrap(),
                    None,
                    &vec![
                        PaginationField::TotalCount,
                        PaginationField::EndCursor,
                        PaginationField::HasNextPage,
                    ],
                ),
                &Select::new(&["venues".into()]),
                &Filter::default(),
                &Order::default(),
            );

            let (pagination_data, documents) = node
                .context
                .store
                .query(&visited_schema, &args, None)
                .await
                .expect("Query failed");
        });
    }

    #[rstest]
    fn relation_list_pagination_over_ordered_view_ids(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let (venues_schema, venues_view_ids) =
                create_venues_test_data(&mut node, &key_pair).await;

            let (visited_schema, visited_view_ids) = create_visited_test_data(
                &mut node,
                venues_view_ids.clone(),
                venues_schema.to_owned(),
                &key_pair,
            )
            .await;

            let mut view_ids = [
                venues_view_ids[0].clone(),
                venues_view_ids[0].clone(),
                venues_view_ids[1].clone(),
                venues_view_ids[2].clone(),
                venues_view_ids[0].clone(),
                venues_view_ids[0].clone(),
                venues_view_ids[1].clone(),
            ];
            let view_ids_len = view_ids.len();

            // Sort created documents by document view id, to compare to similarily sorted query
            // results
            view_ids.sort();

            let mut cursor: Option<PaginationCursor> = None;

            // Select the pinned relation list "venues" of the second visited document
            let list = RelationList::new_pinned(&visited_view_ids[0], "venues".into());

            let mut args = Query::new(
                &Pagination::new(
                    &NonZeroU64::new(1).unwrap(),
                    cursor.as_ref(),
                    &vec![
                        PaginationField::TotalCount,
                        PaginationField::EndCursor,
                        PaginationField::HasNextPage,
                    ],
                ),
                &Select::new(&[Field::Meta(MetaField::DocumentViewId)]),
                &Filter::default(),
                &Order::new(
                    &Field::Meta(MetaField::DocumentViewId),
                    &Direction::Ascending,
                ),
            );

            // Go through all pages, one document at a time
            for (index, view_id) in view_ids.iter().enumerate() {
                args.pagination.after = cursor;

                let (pagination_data, documents) = node
                    .context
                    .store
                    .query(&venues_schema, &args, Some(&list))
                    .await
                    .expect("Query failed");

                match pagination_data.end_cursor {
                    Some(end_cursor) => {
                        cursor = Some(end_cursor);
                    }
                    None => panic!("Expected cursor"),
                }

                if view_ids_len - 1 == index {
                    assert_eq!(pagination_data.has_next_page, false);
                } else {
                    assert_eq!(pagination_data.has_next_page, true);
                }

                assert_eq!(pagination_data.total_count, Some(7));
                assert_eq!(documents.len(), 1);
                assert_eq!(&documents[0].1.view_id, view_id);
                assert_eq!(cursor.as_ref(), Some(&documents[0].0));
            }

            // Query one last time after we paginated through everything
            args.pagination.after = cursor;

            let (pagination_data, documents) = node
                .context
                .store
                .query(&venues_schema, &args, Some(&list))
                .await
                .expect("Query failed");

            assert_eq!(pagination_data.total_count, Some(7));
            assert_eq!(pagination_data.end_cursor, None);
            assert_eq!(pagination_data.has_next_page, false);
            assert_eq!(documents.len(), 0);
        });
    }

    #[rstest]
    #[case::default(
        Filter::default(),
        Order::default(),
        vec![
            "World Wide Feld".to_string(),
            "World Wide Feld".to_string(),
            "Internet Explorer".to_string(),
            "p4p space".to_string(),
            "World Wide Feld".to_string(),
            "World Wide Feld".to_string(),
            "Internet Explorer".to_string(),
        ]
    )]
    #[case::order_by_name(
        Filter::default(),
        Order::new(&"name".into(), &Direction::Ascending),
        vec![
            "Internet Explorer".to_string(),
            "Internet Explorer".to_string(),
            "p4p space".to_string(),
            "World Wide Feld".to_string(),
            "World Wide Feld".to_string(),
            "World Wide Feld".to_string(),
            "World Wide Feld".to_string(),
        ]
    )]
    #[case::search_for_text(
        Filter::new().fields(&[("name_contains", &["p".into()])]),
        Order::default(),
        vec![
            "Internet Explorer".to_string(),
            "p4p space".to_string(),
            "Internet Explorer".to_string(),
        ]
    )]
    #[case::filter_and_order_by_name(
        Filter::new().fields(&[("name_in", &["World Wide Feld".into(), "Internet Explorer".into()])]),
        Order::new(&"name".into(), &Direction::Descending),
        vec![
            "World Wide Feld".to_string(),
            "World Wide Feld".to_string(),
            "World Wide Feld".to_string(),
            "World Wide Feld".to_string(),
            "Internet Explorer".to_string(),
            "Internet Explorer".to_string(),
        ]
    )]
    fn paginated_pinned_relation_list(
        key_pair: KeyPair,
        #[case] filter: Filter,
        #[case] order: Order,
        #[case] expected_venues: Vec<String>,
    ) {
        test_runner(|mut node: TestNode| async move {
            let (venues_schema, mut venues_view_ids) =
                create_venues_test_data(&mut node, &key_pair).await;

            let (visited_schema, mut visited_view_ids) = create_visited_test_data(
                &mut node,
                venues_view_ids.clone(),
                venues_schema.clone(),
                &key_pair,
            )
            .await;

            let documents_len = expected_venues.len() as u64;

            let mut cursor: Option<PaginationCursor> = None;

            // Select the pinned relation list "venues" of the first visited document
            let list = RelationList::new_pinned(&visited_view_ids[0], "venues".into());

            let mut args = Query::new(
                &Pagination::new(
                    &NonZeroU64::new(1).unwrap(),
                    cursor.as_ref(),
                    &vec![
                        PaginationField::TotalCount,
                        PaginationField::EndCursor,
                        PaginationField::HasNextPage,
                    ],
                ),
                &Select::new(&[
                    Field::Meta(MetaField::DocumentViewId),
                    Field::Meta(MetaField::Owner),
                    "name".into(),
                ]),
                &filter,
                &order,
            );

            // Go through all pages, one document at a time
            for (index, expected_venue) in expected_venues.into_iter().enumerate() {
                args.pagination.after = cursor;

                let (pagination_data, documents) = node
                    .context
                    .store
                    .query(&venues_schema, &args, Some(&list))
                    .await
                    .expect("Query failed");

                // Check if next cursor exists and prepare it for next iteration
                match pagination_data.end_cursor {
                    Some(end_cursor) => {
                        cursor = Some(end_cursor);
                    }
                    None => panic!("Expected cursor"),
                }

                // Check if `has_next_page` flag is correct
                if documents_len - 1 == index as u64 {
                    assert_eq!(pagination_data.has_next_page, false);
                } else {
                    assert_eq!(pagination_data.has_next_page, true);
                }

                // Check if pagination info is correct
                assert_eq!(pagination_data.total_count, Some(documents_len));
                assert_eq!(cursor.as_ref(), Some(&documents[0].0));

                // Check if resulting document is correct
                assert_eq!(documents[0].1.author(), &key_pair.public_key());
                assert_eq!(documents.len(), 1);
                assert_eq!(
                    get_document_value(&documents[0].1, "name"),
                    expected_venue.into()
                );
            }

            // Go to final, empty page
            args.pagination.after = cursor;

            let (pagination_data, documents) = node
                .context
                .store
                .query(&venues_schema, &args, Some(&list))
                .await
                .expect("Query failed");

            assert_eq!(pagination_data.has_next_page, false);
            assert_eq!(pagination_data.total_count, Some(documents_len));
            assert_eq!(pagination_data.end_cursor, None);
            assert_eq!(documents.len(), 0);
        });
    }

    #[rstest]
    #[case::default(Filter::default(), 3)]
    #[case::filtered(Filter::new().fields(&[("name_contains", &["Internet".into()])]), 1)]
    #[case::no_results(Filter::new().fields(&[("name", &["doesnotexist".into()])]), 0)]
    fn count(#[case] filter: Filter, #[case] expected_result: u64, key_pair: KeyPair) {
        test_runner(move |mut node: TestNode| async move {
            let (venues_schema, mut venues_view_ids) =
                create_venues_test_data(&mut node, &key_pair).await;

            let args = Query::new(
                &Pagination::default(),
                &Select::default(),
                &filter,
                &Order::default(),
            );

            let result = node
                .context
                .store
                .count(&venues_schema, &args, None)
                .await
                .unwrap();

            assert_eq!(result, expected_result);
        });
    }

    #[rstest]
    #[case::default(Filter::default(), 7)]
    #[case::filtered_1(Filter::new().fields(&[("name_contains", &["Internet".into()])]), 2)]
    #[case::filtered_2(Filter::new().fields(&[("name_not", &["World Wide Feld".into()])]), 3)]
    #[case::no_results(Filter::new().fields(&[("name", &["doesnotexist".into()])]), 0)]
    fn count_relation_list(
        #[case] filter: Filter,
        #[case] expected_result: u64,
        key_pair: KeyPair,
    ) {
        test_runner(move |mut node: TestNode| async move {
            let (venues_schema, mut venues_view_ids) =
                create_venues_test_data(&mut node, &key_pair).await;

            let (visited_schema, mut visited_view_ids) = create_visited_test_data(
                &mut node,
                venues_view_ids.clone(),
                venues_schema.clone(),
                &key_pair,
            )
            .await;

            let args = Query::new(
                &Pagination::default(),
                &Select::default(),
                &filter,
                &Order::default(),
            );

            // Select the pinned relation list "venues" of the first visited document
            let list = RelationList::new_pinned(&visited_view_ids[0], "venues".into());

            let result = node
                .context
                .store
                .count(&venues_schema, &args, Some(&list))
                .await
                .unwrap();

            assert_eq!(result, expected_result);
        });
    }

    #[rstest]
    fn total_count_of_document_with_relation_list_field(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let (venues_schema, mut venues_view_ids) =
                create_venues_test_data(&mut node, &key_pair).await;

            let (visited_schema, mut visited_view_ids) = create_visited_test_data(
                &mut node,
                venues_view_ids,
                venues_schema.to_owned(),
                &key_pair,
            )
            .await;

            let args = Query::new(
                &Pagination::new(
                    &NonZeroU64::new(25).unwrap(),
                    None,
                    &vec![PaginationField::TotalCount],
                ),
                &Select::new(&[Field::Meta(MetaField::DocumentId)]),
                &Filter::default(),
                &Order::default(),
            );

            let (pagination_data, documents) = node
                .context
                .store
                .query(&visited_schema, &args, None)
                .await
                .expect("Query failed");

            assert_eq!(documents.len(), 2);
            assert_eq!(pagination_data.total_count, Some(2));
        });
    }

    #[rstest]
    fn select_cursor_during_conversion(schema_id: SchemaId) {
        let relation_list_hash = Hash::new_from_bytes(&[0]).to_string();
        let first_document_hash = Hash::new_from_bytes(&[1]).to_string();
        let second_document_hash = Hash::new_from_bytes(&[2]).to_string();

        let root_cursor_1 = Hash::new_from_bytes(&[0, 1]).to_string();
        let root_cursor_2 = Hash::new_from_bytes(&[0, 2]).to_string();
        let cursor_1 = Hash::new_from_bytes(&[0, 3]).to_string();
        let cursor_2 = Hash::new_from_bytes(&[0, 4]).to_string();

        let query_rows = vec![
            // First document
            QueryRow {
                document_id: first_document_hash.clone(),
                document_view_id: first_document_hash.clone(),
                operation_id: first_document_hash.clone(),
                is_deleted: false,
                is_edited: false,
                root_cursor: root_cursor_1.clone(),
                cmp_value_cursor: cursor_1.clone(),
                owner: OptionalOwner::default(),
                name: "username".to_string(),
                value: Some("panda".to_string()),
                field_type: "str".to_string(),
                list_index: 0,
            },
            QueryRow {
                document_id: first_document_hash.clone(),
                document_view_id: first_document_hash.clone(),
                operation_id: first_document_hash.clone(),
                is_deleted: false,
                is_edited: false,
                root_cursor: root_cursor_1.clone(),
                cmp_value_cursor: cursor_1.clone(),
                owner: OptionalOwner::default(),
                name: "is_admin".to_string(),
                value: Some("false".to_string()),
                field_type: "bool".to_string(),
                list_index: 0,
            },
            // Second document
            QueryRow {
                document_id: second_document_hash.clone(),
                document_view_id: second_document_hash.clone(),
                operation_id: second_document_hash.clone(),
                is_deleted: false,
                is_edited: false,
                root_cursor: root_cursor_2.clone(),
                cmp_value_cursor: cursor_2.clone(),
                owner: OptionalOwner::default(),
                name: "username".to_string(),
                value: Some("penguin".to_string()),
                field_type: "str".to_string(),
                list_index: 0,
            },
            QueryRow {
                document_id: second_document_hash.clone(),
                document_view_id: second_document_hash.clone(),
                operation_id: second_document_hash.clone(),
                is_deleted: false,
                is_edited: false,
                root_cursor: root_cursor_2.clone(),
                cmp_value_cursor: cursor_2.clone(),
                owner: OptionalOwner::default(),
                name: "is_admin".to_string(),
                value: Some("true".to_string()),
                field_type: "bool".to_string(),
                list_index: 0,
            },
        ];

        // convert as if this is a relation list query
        let result = convert_rows(
            query_rows.clone(),
            Some(&RelationList::new_unpinned(
                &relation_list_hash.parse().unwrap(),
                "relation_list_field",
            )),
            &vec!["username".to_string(), "is_admin".to_string()],
            &schema_id,
        );

        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0].0,
            PaginationCursor::new(
                OperationCursor::from(cursor_1.as_str()),
                Some(OperationCursor::from(root_cursor_1.as_str())),
                Some(relation_list_hash.parse().unwrap())
            )
        );
        assert_eq!(
            result[1].0,
            PaginationCursor::new(
                OperationCursor::from(cursor_2.as_str()),
                Some(OperationCursor::from(root_cursor_2.as_str())),
                Some(relation_list_hash.parse().unwrap())
            )
        );

        // convert as if this is _not_ a relation list query
        let result = convert_rows(
            query_rows,
            None,
            &vec!["username".to_string(), "is_admin".to_string()],
            &schema_id,
        );

        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0].0,
            PaginationCursor::new(OperationCursor::from(cursor_1.as_str()), None, None)
        );
        assert_eq!(
            result[1].0,
            PaginationCursor::new(OperationCursor::from(cursor_2.as_str()), None, None)
        );
    }
}
