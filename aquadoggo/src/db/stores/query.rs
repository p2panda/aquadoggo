// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module offers a query API to find many p2panda documents, filtered or sorted by custom
//! parameters. The results are batched via cursor-based pagination.
use std::collections::HashMap;
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
    Cursor, Direction, Field, Filter, FilterBy, FilterSetting, LowerBound, MetaField, Order,
    Pagination, PaginationField, Select, UpperBound,
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
        Self::new(row.list_index as u64, document_id, None)
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
#[derive(Default, Debug, Clone)]
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
        .expect("Field name not given in Schema");

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
        OperationValue::Boolean(value) => (if *value { "1" } else { "0" }).to_string(),
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

fn filter_sql(filter: &Filter) -> String {
    filter
        .iter()
        .filter_map(|filter_setting| {
            match &filter_setting.field {
                Field::Meta(MetaField::Owner) => {
                    Some(format!("AND {}", cmp_sql("owner", filter_setting)))
                }
                Field::Meta(MetaField::Edited) => {
                    Some(format!("AND {}", cmp_sql("is_edited", filter_setting)))
                }
                Field::Meta(MetaField::Deleted) => {
                    // @TODO: Make sure that deleted is by default always set to false,
                    // this can be handled on a layer above this
                    if let FilterBy::Element(OperationValue::Boolean(filter_value)) =
                        filter_setting.by
                    {
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
                    let filter_cmp = cmp_sql("operation_fields_v1.value", filter_setting);

                    Some(format!(
                        r#"
                        AND EXISTS (
                            SELECT
                                value
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

fn pagination_sql(
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
                Field::Meta(MetaField::DocumentId) => {
                    format!("AND documents.document_view_id > '{view_id}'")
                }
                Field::Meta(MetaField::DocumentViewId) => {
                    format!("AND documents.document_view_id > '{view_id}'")
                }
                Field::Meta(MetaField::Owner) => {
                    todo!("Not implemented yet");
                }
                Field::Meta(MetaField::Edited) => {
                    todo!("Not implemented yet");
                }
                Field::Meta(MetaField::Deleted) => {
                    todo!("not implemented yet");
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

                    format!(
                        r#"
                        AND EXISTS (
                            SELECT
                                operation_fields_v1.value,

                                -- When ordering is activated we need to compare against the value
                                -- of the ordered field - but from the row where the cursor points at
                                (
                                    SELECT
                                        operation_fields_v1.value
                                    FROM
                                        {from}

                                        JOIN
                                            operation_fields_v1
                                            ON
                                                document_view_fields.operation_id = operation_fields_v1.operation_id
                                    WHERE
                                        operation_fields_v1.name = '{order_field_name}'
                                        AND
                                            document_view_fields.document_view_id = '{view_id}'
                                        {and_list_index}

                                ) AS cmp_value

                            FROM
                                operation_fields_v1

                            WHERE
                                operation_fields_v1.name = '{order_field_name}'

                                AND
                                    operation_fields_v1.operation_id = document_view_fields.operation_id

                                AND
                                    (
                                        operation_fields_v1.value > cmp_value
                                        OR
                                        (
                                            operation_fields_v1.value = cmp_value
                                            AND
                                                documents.document_view_id > '{view_id}'
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

fn order_sql(order: &Order, schema: &Schema) -> String {
    let direction = match order.direction {
        Direction::Ascending => "ASC",
        Direction::Descending => "DESC",
    };

    match &order.field {
        Field::Meta(MetaField::DocumentId) => {
            format!("ORDER BY documents.document_id {direction}, list_index ASC")
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

fn limit_sql<C>(pagination: &Pagination<C>, fields: &Vec<String>) -> (u64, String)
where
    C: Cursor,
{
    // We multiply the value by the number of fields we selected. If no fields have been selected
    // we just take the page size as is
    let page_size = pagination.first.get() * std::cmp::max(1, fields.len() as u64);

    // ... and add + 1 for the "has next page" flag
    (page_size, format!("LIMIT {page_size} + 1"))
}

fn application_select_sql(fields: &Vec<String>) -> String {
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

fn total_count_sql(schema_id: &SchemaId, list: Option<&RelationList>, filter: &Filter) -> String {
    let from = from_sql(list);
    let and_where = where_sql(schema_id, list);
    let and_filters = filter_sql(filter);

    format!(
        r#"
            SELECT
                COUNT(documents.document_id)

            FROM
                {from}

                JOIN document_view_fields
                    ON documents.document_view_id = document_view_fields.document_view_id
                JOIN operation_fields_v1
                    ON
                        document_view_fields.operation_id = operation_fields_v1.operation_id
                        AND
                            document_view_fields.name = operation_fields_v1.name

            WHERE
                {and_where}
                {and_filters}

            -- Group application fields by name to make sure we get actual number of documents
            GROUP BY operation_fields_v1.name
        "#
    )
}

fn edited_sql(select: &Select) -> String {
    if select.fields.contains(&Field::Meta(MetaField::Edited)) {
        r#"
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
            )
        "#
        .to_string()
    } else {
        // Use constant when we do not query for edited status. This is useful for keeping the
        // resulting SQL row type in shape.
        "0".to_string()
    }
}

fn owner_sql(select: &Select) -> String {
    if select.fields.contains(&Field::Meta(MetaField::Owner)) {
        r#"
            -- The original owner of a document we get by checking which public key signed the
            -- "create" operation, the hash of that operation is the same as the document id
            (
                SELECT
                    operations_v1.public_key
                FROM
                    operations_v1
                WHERE
                    operations_v1.operation_id = documents.document_id
            )
        "#
        .to_string()
    } else {
        // Use constant when we do not query owner. This is useful for keeping the resulting SQL
        // row type in shape.
        //
        // The value looks funny and exists to still pass as a ed25519 public key.
        "'0000000000000000000000000000000000000000000000000000000000000000'".to_string()
    }
}

fn where_sql(schema_id: &SchemaId, list: Option<&RelationList>) -> String {
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
                "#
            )
        }
        None => {
            // We filter by the queried schema of that collection
            format!("documents.schema_id = '{schema_id}'")
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

            // Select relation list of parent document first and join the related documents
            // afterwards
            format!(
                r#"
                    document_view_fields document_view_fields_list
                    JOIN operation_fields_v1 operation_fields_v1_list
                        ON
                            document_view_fields_list.operation_id = operation_fields_v1_list.operation_id
                        AND
                            document_view_fields_list.name = operation_fields_v1_list.name
                    JOIN documents
                        ON
                            operation_fields_v1_list.value = {filter_sql}
                "#
            )
        }
        // Otherwise just query the documents directly
        None => "documents".to_string(),
    }
}

fn list_index_sql(list: Option<&RelationList>) -> String {
    match list {
        // Use the list index of the parent document when we query a relation list
        Some(_) => "operation_fields_v1_list.list_index AS list_index".to_string(),
        None => "operation_fields_v1.list_index AS list_index".to_string(),
    }
}

fn group_sql(list: Option<&RelationList>) -> String {
    match list {
        // Include list index of the parent document relation list to allow duplicate documents
        Some(_) => "GROUP BY documents.document_id, operation_fields_v1.operation_id, operation_fields_v1.name, operation_fields_v1_list.list_index".to_string(),
        None => "GROUP BY documents.document_id, operation_fields_v1.operation_id, operation_fields_v1.name".to_string(),
    }
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
        let application_fields: Vec<String> = args
            .select
            .iter()
            .filter_map(|field| {
                // Remove all meta fields
                if let Field::Field(field_name) = field {
                    Some(field_name.clone())
                } else {
                    None
                }
            })
            .collect();

        // Generate SQL based on the given schema and query
        let select_edited = edited_sql(&args.select);
        let select_owner = owner_sql(&args.select);
        let select_list_index = list_index_sql(list);

        let from = from_sql(list);

        let where_ = where_sql(schema.id(), list);
        let and_select = application_select_sql(&application_fields);
        let and_filters = filter_sql(&args.filter);
        let and_pagination = pagination_sql(&args.pagination, list, &args.order);

        let group = group_sql(list);
        let order = order_sql(&args.order, schema);
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
                operation_fields_v1.operation_id,

                -- The deletion status of a document we already store in the database, let's select it in
                -- any case since we get it for free
                documents.is_deleted,

                -- Optionally we check for the edited status and owner of the document
                {select_edited} AS is_edited,
                {select_owner} AS owner,

                -- Finally we get the application data by selecting the name, value and type.
                operation_fields_v1.name,
                operation_fields_v1.value,
                operation_fields_v1.field_type,

                -- When we query relation list the index value becomes important, as we need to
                -- determine the position of the document in the list
                {select_list_index}

            FROM
                -- Usually we query the "documents" table first. In case we're looking at a relation
                -- list this is slighly more complicated and we need to do some additional JOINs
                {from}

                -- In any case we want to get the (latest) operation values for each document in
                -- the end
                JOIN document_view_fields
                    ON documents.document_view_id = document_view_fields.document_view_id
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
                {and_select}

                -- .. and further filter the data by custom parameters
                {and_filters}

                -- Lastly we batch all results into smaller chunks via cursor pagination
                {and_pagination}

            -- Never return more than one row per operation field to not break pagination
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

        let total_count = if args
            .pagination
            .fields
            .contains(&PaginationField::TotalCount)
        {
            // Make separate query for getting the total number of documents (without pagination)
            let result: Option<(i32,)> = query_as(&total_count_sql(schema_id, list, &args.filter))
                .fetch_optional(&self.pool)
                .await
                .map_err(|err| DocumentStorageError::FatalStorageError(err.to_string()))?;

            match result {
                Some(result) => Some(result.0 as u64),
                None => Some(0),
            }
        } else {
            None
        };

        // Determine cursors for pagination by looking at beginning and end of results
        let start_cursor = rows.first().map(|row| Self::row_to_cursor(row, list));
        let end_cursor = rows.last().map(|row| Self::row_to_cursor(row, list));

        let pagination_data = PaginationData {
            total_count,
            has_next_page,
            // @TODO: Implement backwards pagination
            has_previous_page: false,
            start_cursor,
            end_cursor,
        };

        // Finally convert everything into the right format
        let documents = Self::convert_rows(rows, list, &application_fields, schema.id());

        Ok((pagination_data, documents))
    }

    /// Merges all operation fields from the database into documents.
    ///
    /// Due to the special table layout we receive one row per operation field in the query.
    /// Usually we need to merge multiple rows / operation fields fields into one document.
    fn convert_rows(
        rows: Vec<QueryRow>,
        list: Option<&RelationList>,
        fields: &Vec<String>,
        schema_id: &SchemaId,
    ) -> Vec<(DocumentCursor, StorageDocument)> {
        let mut converted: Vec<(DocumentCursor, StorageDocument)> = Vec::new();

        if rows.is_empty() {
            converted
        } else {
            let finalize_document = |row: &QueryRow,
                                     collected_fields: Vec<DocumentViewFieldRow>|
             -> (DocumentCursor, StorageDocument) {
                let fields = parse_document_view_field_rows(collected_fields);

                let document = StorageDocument {
                    id: row.document_id.parse().unwrap(),
                    fields: Some(fields),
                    schema_id: schema_id.clone(),
                    view_id: row.document_view_id.parse().unwrap(),
                    author: row.owner.parse().unwrap(),
                    deleted: row.is_deleted,
                };

                let cursor = Self::row_to_cursor(row, list);

                (cursor, document)
            };

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
    use p2panda_rs::schema::FieldType;
    use p2panda_rs::test_utils::fixtures::key_pair;
    use rstest::rstest;

    use crate::db::query::{Direction, Field, Filter, MetaField, Order, Pagination, Select};
    use crate::db::types::StorageDocument;
    use crate::graphql::types::OrderDirection;
    use crate::test_utils::{add_document, add_schema, test_runner, TestNode};

    use super::{DocumentCursor, Query};

    fn get_view_fields(
        view_id: &DocumentViewId,
        fields: &[(&str, OperationValue)],
    ) -> DocumentViewFields {
        let operation_id: OperationId = view_id.to_string().parse().unwrap();
        let mut operation_fields = OperationFields::new();

        for field in fields {
            operation_fields.insert(field.0, field.1.clone());
        }

        DocumentViewFields::new_from_operation_fields(&operation_id, &operation_fields)
    }

    #[rstest]
    fn ordered_query(key_pair: KeyPair) {
        test_runner(|mut node: TestNode| async move {
            let events_schema = add_schema(
                &mut node,
                "events",
                vec![
                    ("title", FieldType::String),
                    ("date", FieldType::String),
                    ("ticket_price", FieldType::Float),
                ],
                &key_pair,
            )
            .await;

            let first_document = add_document(
                &mut node,
                events_schema.id(),
                vec![
                    ("title", "Kids Bits! Chiptune for baby squirrels".into()),
                    ("date", "2023-04-17".into()),
                    ("ticket_price", 5.75.into()),
                ],
                &key_pair,
            )
            .await;

            let second_document = add_document(
                &mut node,
                events_schema.id(),
                vec![
                    ("title", "The Pandadoodle Flute Trio".into()),
                    ("date", "2023-04-14".into()),
                    ("ticket_price", 12.5.into()),
                ],
                &key_pair,
            )
            .await;

            let args = Query::new(
                &Pagination::default(),
                &Select::new(&["title".into()]),
                &Filter::new(),
                &Order::new(&"date".into(), &Direction::Ascending),
            );

            let (pagination_data, documents) = node
                .context
                .store
                .query(&events_schema, &args, None)
                .await
                .unwrap();

            assert_eq!(documents.len(), 2);
            assert_eq!(
                documents[0].1.fields(),
                Some(&get_view_fields(
                    &second_document,
                    &[("title", "The Pandadoodle Flute Trio".into())]
                ))
            );
            assert_eq!(
                documents[1].1.fields(),
                Some(&get_view_fields(
                    &first_document,
                    &[("title", "Kids Bits! Chiptune for baby squirrels".into())]
                ))
            );
        });
    }
}
