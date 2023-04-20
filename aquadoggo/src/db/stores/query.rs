// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module offers a query API to find many p2panda documents, filtered or sorted by custom
//! parameters. The results are batched via cursor-based pagination.
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use anyhow::bail;
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
use crate::db::types::StorageDocument;
use crate::db::SqlStore;
use crate::graphql::types::PaginationData;

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
pub struct DocumentCursor {
    /// Id aiding us to determine the current row.
    pub document_view_id: DocumentViewId,

    /// This value is not interesting for collection queries, it will always be "0". For relation
    /// list queries we need it though as we might have duplicate document view ids in the list,
    /// through the list index we can keep them apart from each other.
    pub list_index: u64,

    /// In relation list queries we use this field to represent the parent document holding that
    /// list.
    pub root: Option<DocumentViewId>,
}

impl DocumentCursor {
    pub fn new(
        list_index: u64,
        document_view_id: DocumentViewId,
        root: Option<DocumentViewId>,
    ) -> Self {
        Self {
            list_index,
            document_view_id,
            root,
        }
    }
}

impl From<&QueryRow> for DocumentCursor {
    fn from(row: &QueryRow) -> Self {
        let document_id = row.document_id.parse().unwrap();
        let list_index = row.list_index;
        Self::new(list_index as u64, document_id, None)
    }
}

// This should _not_ be an underscore character since we're also parsing document view ids in the
// string which contain that character already
const CURSOR_SEPARATOR: char = '-';

impl Cursor for DocumentCursor {
    type Error = anyhow::Error;

    fn decode(encoded: &str) -> Result<Self, Self::Error> {
        let bytes = bs58::decode(encoded).into_vec()?;
        let decoded = std::str::from_utf8(&bytes)?;

        let parts: Vec<&str> = decoded.split(CURSOR_SEPARATOR).collect();
        match parts.len() {
            2 => Ok(Self::new(
                u64::from_str(parts[0])?,
                DocumentViewId::from_str(parts[1])?,
                None,
            )),
            3 => Ok(Self::new(
                u64::from_str(parts[0])?,
                DocumentViewId::from_str(parts[1])?,
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
                "{}{}{}{}",
                self.list_index,
                CURSOR_SEPARATOR,
                self.document_view_id,
                self.root.as_ref().map_or("".to_string(), |view_id| format!(
                    "{}{}",
                    CURSOR_SEPARATOR, view_id
                ))
            )
            .as_bytes(),
        )
        .into_string()
    }
}

pub type QueryResponse = (
    PaginationData<DocumentCursor>,
    Vec<(DocumentCursor, StorageDocument)>,
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
fn typecast_field_sql(sql_field: &str, field_name: &str, schema: &Schema) -> String {
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
        _ => sql_field.to_string(),
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
                    let field_sql = typecast_field_sql("operation_fields_v1.value", field_name, schema);
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
    pagination: &Pagination<DocumentCursor>,
    list: Option<&RelationList>,
    order: &Order,
) -> String {
    // Ignore pagination if we're in a relation list query and the cursor does not match the parent
    // document view id
    if let (Some(relation_list), Some(cursor)) = (list, pagination.after.as_ref()) {
        if Some(&relation_list.root) != cursor.root.as_ref() {
            return "".to_string();
        }
    }

    match &pagination.after {
        Some(cursor) => {
            let view_id = cursor.document_view_id.to_string();
            let list_index = cursor.list_index;

            match &order.field {
                Field::Meta(MetaField::DocumentId) | Field::Meta(MetaField::DocumentViewId) => {
                    match list {
                        Some(relation_list) => {
                            format!("AND operation_fields_v1_list.list_index > {list_index}")
                        }
                        None => format!("AND documents.document_view_id > '{view_id}'"),
                    }
                }
                Field::Meta(MetaField::Owner)
                | Field::Meta(MetaField::Edited)
                | Field::Meta(MetaField::Deleted) => {
                    // @TODO: See issue: https://github.com/p2panda/aquadoggo/issues/326
                    todo!("Not implemented yet");
                }
                Field::Field(order_field_name) => {
                    let from = match list {
                        Some(relation_list) => {
                            r#"
                            document_view_fields document_view_fields_list

                            JOIN operation_fields_v1 operation_fields_v1_list
                                ON
                                    document_view_fields_list.operation_id = operation_fields_v1_list.operation_id
                                AND
                                    document_view_fields_list.name = operation_fields_v1_list.name

                            JOIN document_view_fields
                                ON
                                    operation_fields_v1_list.value = document_view_fields.document_view_id
                            "#.to_string()
                        }
                        None => "document_view_fields".to_string(),
                    };

                    let and_list_index = match list {
                        Some(relation_list) => {
                            format!("AND operation_fields_v1_list.list_index = {list_index}")
                        }
                        None => "".to_string(),
                    };

                    let cmp_value = format!(
                        r#"
                        -- When ordering is activated we need to compare against the value
                        -- of the ordered field - but from the row where the cursor points at
                        SELECT
                            operation_fields_v1.value
                        FROM
                            {from}
                            JOIN
                                operation_fields_v1
                                ON
                                    document_view_fields.operation_id = operation_fields_v1.operation_id
                                    AND
                                        document_view_fields.name = operation_fields_v1.name
                        WHERE
                            operation_fields_v1.name = '{order_field_name}'
                            AND
                                document_view_fields.document_view_id = '{view_id}'
                            {and_list_index}
                        "#
                    );

                    let and_list_index_gt = match list {
                        Some(relation_list) => {
                            format!("AND operation_fields_v1_list.list_index > {list_index}")
                        }
                        None => "".to_string(),
                    };

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
                                        operation_fields_v1.value > ({cmp_value})
                                        OR
                                        (
                                            operation_fields_v1.value = ({cmp_value})
                                            AND
                                                documents.document_view_id > '{view_id}'
                                            {and_list_index_gt}
                                        )
                                    )
                        )
                        "#
                    )
                }
            }
        }
        None => "".to_string(),
    }
}

fn group_sql(fields: &ApplicationFields) -> String {
    if fields.is_empty() {
        r#"
        -- Make sure to only return one row per document when no fields have been selected
        GROUP BY documents.document_id, document_view_fields.operation_id, list_index
        "#
        .to_string()
    } else {
        "".to_string()
    }
}

fn order_sql(order: &Order, schema: &Schema, list: Option<&RelationList>) -> String {
    let direction = match order.direction {
        Direction::Ascending => "ASC",
        Direction::Descending => "DESC",
    };

    let list_sql = match list {
        Some(_) => "operation_fields_v1_list.list_index ASC,",
        None => "",
    };

    match &order.field {
        Field::Meta(MetaField::DocumentId) => {
            format!("ORDER BY {list_sql} documents.document_id {direction}")
        }
        Field::Meta(meta_field) => {
            let sql_field = match meta_field {
                MetaField::DocumentViewId => "documents.document_view_id",
                MetaField::Owner => "owner",
                MetaField::Edited => "is_edited",
                MetaField::Deleted => "documents.is_deleted",
                // Other cases are handled above
                _ => panic!("Unhandled meta field order case"),
            };

            format!(
                r#"
                ORDER BY
                    {sql_field} {direction},

                    -- .. and by relation list index, in case we're querying one
                    {list_sql}

                    -- On top we sort always by id in case the previous order
                    -- value is equal between two rows
                    documents.document_id ASC
                "#
            )
        }
        Field::Field(field_name) => {
            format!(
                r#"
                ORDER BY
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
                    {},

                    -- .. and by relation list index, in case we're querying one
                    {list_sql}

                    -- On top we sort always by view id in case the previous order
                    -- value is equal between two rows
                    documents.document_view_id ASC
                "#,
                typecast_field_sql("value", field_name, schema),
                field_name,
                direction,
            )
        }
    }
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

fn select_edited_sql(select: &Select) -> String {
    if select.fields.contains(&Field::Meta(MetaField::Edited)) {
        r#"
        -- Check if there is more operations next to the initial "create" operation
        ,
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
        .to_string()
    } else {
        "".to_string()
    }
}

fn select_owner_sql(select: &Select) -> String {
    if select.fields.contains(&Field::Meta(MetaField::Owner)) {
        r#"
        -- The original owner of a document we get by checking which public key signed the
        -- "create" operation, the hash of that operation is the same as the document id
        ,
        (
            SELECT
                operations_v1.public_key
            FROM
                operations_v1
            WHERE
                operations_v1.operation_id = documents.document_id
        ) AS owner
        "#
        .to_string()
    } else {
        "".to_string()
    }
}

fn select_fields_sql(fields: &ApplicationFields, list: Option<&RelationList>) -> String {
    let select_list_index = match list {
        // Use the list index of the parent document when we query a relation list
        Some(_) => "operation_fields_v1_list.list_index AS list_index".to_string(),
        // In any other case this will always be 0
        None => "0 AS list_index".to_string(),
    };

    if fields.is_empty() {
        format!(", {select_list_index}")
    } else {
        format!(
            r#"
            -- Finally we get the application data by selecting the name, value and type.
            ,
            operation_fields_v1.name,
            operation_fields_v1.value,
            operation_fields_v1.field_type,

            -- When we query relation list the index value becomes important, as we need to
            -- determine the position of the document in the list
            {select_list_index}
            "#
        )
    }
}

fn where_sql(
    schema_id: &SchemaId,
    fields: &ApplicationFields,
    list: Option<&RelationList>,
) -> String {
    let list_index_sql = if fields.is_empty() {
        ""
    } else {
        "AND
            -- Only one row per field: restrict relation lists to first list item
            operation_fields_v1.list_index = 0
        "
    };

    match list {
        Some(relation_list) => {
            // We filter by the parent document view id of this relation list
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
                "#
            )
        }
        None => {
            // We filter by the queried schema of that collection
            format!(
                r#"
                documents.schema_id = '{schema_id}'
                {list_index_sql}
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

fn from_fields_sql(fields: &ApplicationFields) -> String {
    if fields.is_empty() {
        ""
    } else {
        r#"
        JOIN operation_fields_v1
            ON
                document_view_fields.operation_id = operation_fields_v1.operation_id
                AND
                    document_view_fields.name = operation_fields_v1.name
        "#
    }
    .to_string()
}

impl SqlStore {
    /// Returns a paginated collection of documents from the database which can be filtered and
    /// ordered by custom parameters.
    ///
    /// When passing a `list` configuration the query will run against the documents of a (pinned)
    /// relation list instead.
    pub async fn query(
        &self,
        schema: &Schema,
        args: &Query<DocumentCursor>,
        list: Option<&RelationList>,
    ) -> Result<QueryResponse, DocumentStorageError> {
        let schema_id = schema.id();

        // Get all selected application fields from query
        let application_fields = args.select.application_fields();

        // Generate SQL based on the given schema and query
        let select_edited = select_edited_sql(&args.select);
        let select_owner = select_owner_sql(&args.select);
        let select_fields = select_fields_sql(&application_fields, list);

        let from = from_sql(list);
        let from_fields = from_fields_sql(&application_fields);

        let where_ = where_sql(schema.id(), &application_fields, list);
        let and_fields = where_fields_sql(&application_fields);
        let and_filters = where_filter_sql(&args.filter, schema);
        let and_pagination = where_pagination_sql(&args.pagination, list, &args.order);

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
                -- We get all the meta informations first, let's start with the document id,
                -- document view id and id of the operation which holds the data
                documents.document_id,
                documents.document_view_id,
                document_view_fields.operation_id,

                -- The deletion status of a document we already store in the database, let's select it in
                -- any case since we get it for free
                documents.is_deleted

                -- All other fields we optionally select depending on the query
                {select_edited}
                {select_owner}
                {select_fields}

            FROM
                -- Usually we query the "documents" table first. In case we're looking at a relation
                -- list this is slighly more complicated and we need to do some additional JOINs
                {from}

                -- If we queried application fields, we need to add some more JOINs to get the
                -- values from the operations
                {from_fields}

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

            -- If we queried only meta data we need to group the resulting rows by document id,
            -- to assure we only receive one row per document
            {group}

            -- We always order the rows by document id and list index, but there might also be
            -- user-defined ordering on top of that
            {order}

            -- Connected to cursor pagination we limit the number of rows
            {limit}
        "#
        );

        // @TODO: Remove this
        println!("{sea_quel}");

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

        // Determine cursors for pagination by looking at beginning and end of results
        let start_cursor = if args
            .pagination
            .fields
            .contains(&PaginationField::StartCursor)
        {
            rows.first().map(|row| Self::row_to_cursor(row, list))
        } else {
            None
        };

        let end_cursor = if args.pagination.fields.contains(&PaginationField::EndCursor) {
            rows.last().map(|row| Self::row_to_cursor(row, list))
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

        // Finally convert everything into the right format
        let documents = Self::convert_rows(rows, list, &application_fields, schema.id());

        Ok((pagination_data, documents))
    }

    /// Query number of documents in filtered collection.
    pub async fn count(
        &self,
        schema: &Schema,
        args: &Query<DocumentCursor>,
        list: Option<&RelationList>,
    ) -> Result<u64, DocumentStorageError> {
        let application_fields = args.select.application_fields();

        let from = from_sql(list);
        let where_ = where_sql(schema.id(), &application_fields, list);
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

    /// Merges all operation fields from the database into documents.
    ///
    /// Due to the special table layout we receive one row per operation field in the query.
    /// Usually we need to merge multiple rows / operation fields fields into one document.
    fn convert_rows(
        rows: Vec<QueryRow>,
        list: Option<&RelationList>,
        fields: &ApplicationFields,
        schema_id: &SchemaId,
    ) -> Vec<(DocumentCursor, StorageDocument)> {
        let mut converted: Vec<(DocumentCursor, StorageDocument)> = Vec::new();

        if rows.is_empty() {
            return converted;
        }

        // Helper method to convert database row into final document and cursor type
        let finalize_document = |row: &QueryRow,
                                 collected_fields: Vec<DocumentViewFieldRow>|
         -> (DocumentCursor, StorageDocument) {
            let fields = parse_document_view_field_rows(collected_fields);

            let document = StorageDocument {
                id: row.document_id.parse().unwrap(),
                fields: Some(fields),
                schema_id: schema_id.clone(),
                view_id: row.document_view_id.parse().unwrap(),
                author: (&row.owner).into(),
                deleted: row.is_deleted,
            };

            let cursor = Self::row_to_cursor(row, list);

            (cursor, document)
        };

        // Iterate over all database rows and collect fields for each document
        let last_row = rows.last().unwrap().clone();

        let mut current = rows[0].clone();
        let mut current_fields = Vec::new();

        let rows_per_document = std::cmp::max(fields.len(), 1);

        for (index, row) in rows.into_iter().enumerate() {
            // We observed a new document coming up in the next row, time to change
            if index % rows_per_document == 0 && index > 0 {
                // Finalize the current document, convert it and push it into the final array
                let (cursor, document) = finalize_document(&current, current_fields);
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
                list_index: row.list_index,
                field_type: row.field_type,
                value: row.value,
            });
        }

        // Do it one last time at the end for the last document
        let (cursor, document) = finalize_document(&last_row, current_fields);
        converted.push((cursor, document));

        converted
    }

    /// Get a cursor from a document row.
    fn row_to_cursor(row: &QueryRow, list: Option<&RelationList>) -> DocumentCursor {
        match list {
            Some(relation_list) => {
                // If we're querying a relation list then we mention the document view id
                // of the parent document. This helps us later to understand _which_ of the
                // potentially many relation lists we want to paginate
                DocumentCursor::new(
                    row.list_index as u64,
                    row.document_view_id.parse().unwrap(),
                    Some(relation_list.root.clone()),
                )
            }
            None => row.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use p2panda_rs::document::traits::AsDocument;
    use p2panda_rs::document::{DocumentViewFields, DocumentViewId};
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::{OperationFields, OperationId, OperationValue};
    use p2panda_rs::schema::{FieldType, Schema, SchemaId};
    use p2panda_rs::test_utils::fixtures::key_pair;
    use rstest::rstest;
    use sqlx::query_as;

    use crate::db::query::{
        Direction, Field, Filter, MetaField, Order, Pagination, PaginationField, Select,
    };
    use crate::db::stores::RelationList;
    use crate::db::types::StorageDocument;
    use crate::graphql::types::OrderDirection;
    use crate::test_utils::{add_document, add_schema, test_runner, TestNode};

    use super::{DocumentCursor, Query};

    fn get_document_value(document: &StorageDocument, field: &str) -> OperationValue {
        document
            .fields()
            .expect("Expected document fields")
            .get(field)
            .expect(&format!("Expected '{field}' field to exist in document"))
            .value()
            .to_owned()
    }

    async fn add_schema_and_documents(
        node: &mut TestNode,
        schema_name: &str,
        documents: Vec<Vec<(&str, OperationValue, Option<SchemaId>)>>,
        key_pair: &KeyPair,
    ) -> (Schema, Vec<DocumentViewId>) {
        assert!(documents.len() > 0);

        // Look at first document to automatically derive schema
        let schema_fields = documents[0]
            .iter()
            .map(|(field_name, field_value, schema_id)| {
                // Get field type from operation value
                let field_type = match field_value.field_type() {
                    "relation" => FieldType::Relation(schema_id.clone().unwrap()),
                    "pinned_relation" => FieldType::PinnedRelation(schema_id.clone().unwrap()),
                    "relation_list" => FieldType::RelationList(schema_id.clone().unwrap()),
                    "pinned_relation_list" => {
                        FieldType::PinnedRelationList(schema_id.clone().unwrap())
                    }
                    _ => field_value.field_type().parse().unwrap(),
                };

                (*field_name, field_type)
            })
            .collect();

        // Create schema
        let schema = add_schema(node, schema_name, schema_fields, key_pair).await;

        // Add all documents and return created view ids
        let mut view_ids = Vec::new();
        for document in documents {
            let fields = document
                .iter()
                .map(|field| (field.0, field.1.clone()))
                .collect();
            let view_id = add_document(node, schema.id(), fields, key_pair).await;
            view_ids.push(view_id);
        }

        (schema, view_ids)
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
        #[case] args: Query<DocumentCursor>,
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
    fn pagination(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let (schema, mut view_ids) = create_events_test_data(&mut node, &key_pair).await;
            let view_ids_len = view_ids.len();

            // Sort created documents by document view id, to compare to similarily sorted query
            // results
            view_ids.sort();

            let mut cursor: Option<DocumentCursor> = None;

            // Go through all pages, one document at a time
            for (index, view_id) in view_ids.into_iter().enumerate() {
                let args = Query::new(
                    &Pagination::new(
                        &NonZeroU64::new(1).unwrap(),
                        cursor.as_ref(),
                        &vec![PaginationField::TotalCount, PaginationField::EndCursor],
                    ),
                    &Select::new(&[Field::Meta(MetaField::DocumentViewId)]),
                    &Filter::default(),
                    &Order::new(
                        &Field::Meta(MetaField::DocumentViewId),
                        &Direction::Ascending,
                    ),
                );

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
            let args = Query::new(
                &Pagination::new(
                    &NonZeroU64::new(1).unwrap(),
                    cursor.as_ref(),
                    &vec![PaginationField::TotalCount, PaginationField::EndCursor],
                ),
                &Select::default(),
                &Filter::default(),
                &Order::new(
                    &Field::Meta(MetaField::DocumentViewId),
                    &Direction::Ascending,
                ),
            );

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
                    &vec![PaginationField::TotalCount, PaginationField::EndCursor],
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
                .query(&visited_schema, &args, Some(&list))
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
                .query(&visited_schema, &args, Some(&list))
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

            // Select the pinned relation list "venues" of the first visited document
            let list = RelationList::new_pinned(&visited_view_ids[0], "venues".into());

            // Go through all pages, one document at a time
            let mut cursor: Option<DocumentCursor> = None;
            for (index, expected_venue) in expected_venues.into_iter().enumerate() {
                let args = Query::new(
                    &Pagination::new(
                        &NonZeroU64::new(1).unwrap(),
                        cursor.as_ref(),
                        &vec![PaginationField::TotalCount, PaginationField::EndCursor],
                    ),
                    &Select::new(&[
                        Field::Meta(MetaField::DocumentViewId),
                        Field::Meta(MetaField::Owner),
                        "name".into(),
                    ]),
                    &filter,
                    &order,
                );

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
            let args = Query::new(
                &Pagination::new(
                    &NonZeroU64::new(1).unwrap(),
                    cursor.as_ref(),
                    &vec![PaginationField::TotalCount, PaginationField::EndCursor],
                ),
                &Select::new(&[Field::Meta(MetaField::DocumentViewId), "name".into()]),
                &filter,
                &order,
            );

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
}
