// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module offers a query API to find many p2panda documents, filtered or sorted by custom
//! parameters. The results are batched via cursor-based pagination.
use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;

use anyhow::bail;
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::{FieldName, Schema, SchemaId};
use p2panda_rs::storage_provider::error::DocumentStorageError;
use sqlx::query::QueryAs;
use sqlx::query_as;

use crate::db::models::utils::parse_document_view_field_rows;
use crate::db::models::{DocumentViewFieldRow, QueryRow};
use crate::db::query::{
    ApplicationFields, Cursor, Direction, Field, Filter, FilterBy, FilterSetting, LowerBound,
    MetaField, Order, Pagination, PaginationField, Select, UpperBound,
};
use crate::db::stores::OperationCursor;
use crate::db::types::StorageDocument;
use crate::db::{Pool, SqlStore};

/// Configure query to select documents based on a relation list field.
#[derive(Debug)]
pub struct RelationList {
    /// View id of document which holds relation list field.
    pub root_view_id: DocumentViewId,

    /// Field which contains the relation list values.
    pub field: FieldName,

    /// Type of relation list.
    pub list_type: RelationListType,
}

#[derive(Debug)]
pub enum RelationListType {
    Pinned,
    Unpinned,
}

impl RelationList {
    pub fn new_unpinned(root_view_id: &DocumentViewId, field: &str) -> Self {
        Self {
            root_view_id: root_view_id.to_owned(),
            field: field.to_string(),
            list_type: RelationListType::Unpinned,
        }
    }

    pub fn new_pinned(root_view_id: &DocumentViewId, field: &str) -> Self {
        Self {
            root_view_id: root_view_id.to_owned(),
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

impl Display for PaginationCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.encode())
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

/// Values to bind to SQL query.
#[derive(Debug)]
enum BindArgument {
    String(String),
    Integer(i64),
    Float(f64),
}

/// Helper method to bind untrusted arguments to a sqlx `QueryAs` instance.
fn bind_to_query<'q, O>(
    mut query: QueryAs<'q, sqlx::Any, O, sqlx::any::AnyArguments<'q>>,
    args: &'q Vec<BindArgument>,
) -> QueryAs<'q, sqlx::Any, O, sqlx::any::AnyArguments<'q>> {
    for arg in args {
        query = match arg {
            BindArgument::String(value) => query.bind(value),
            BindArgument::Integer(value) => query.bind(value),
            BindArgument::Float(value) => query.bind(value),
        };
    }

    query
}

/// Helper method to convert operation values into a bindable argument representation for SQL.
fn bind_arg(value: &OperationValue) -> Vec<BindArgument> {
    match &value {
        // Note that we wrap boolean values into a string here, as our operation field values are
        // stored as strings in themselves and we can't typecast them to booleans due to a
        // limitation in SQLite (see typecast_field_sql method).
        //
        // When comparing meta fields like `is_edited` etc. do _not_ use this method since we're
        // dealing there with native boolean values instead of strings.
        OperationValue::Boolean(value) => vec![BindArgument::String(
            (if *value { "true" } else { "false" }).to_string(),
        )],
        OperationValue::Integer(value) => vec![BindArgument::Integer(value.to_owned())],
        OperationValue::Float(value) => vec![BindArgument::Float(value.to_owned())],
        OperationValue::String(value) => vec![BindArgument::String(value.to_owned())],
        OperationValue::Relation(value) => {
            vec![BindArgument::String(value.document_id().to_string())]
        }
        OperationValue::RelationList(value) => value
            .iter()
            .map(|document_id| BindArgument::String(document_id.to_string()))
            .collect(),
        OperationValue::PinnedRelation(value) => {
            vec![BindArgument::String(value.view_id().to_string())]
        }
        OperationValue::PinnedRelationList(value) => value
            .iter()
            .map(|view_id| BindArgument::String(view_id.to_string()))
            .collect(),
        OperationValue::Bytes(value) => vec![BindArgument::String(hex::encode(value))],
    }
}

/// Helper method to convert filter settings into SQL comparison operations.
///
/// Returns the SQL comparison string and adds binding arguments to the mutable array we pass into
/// this function.
fn cmp_sql(
    sql_field: &str,
    filter_setting: &FilterSetting,
    args: &mut Vec<BindArgument>,
) -> String {
    match &filter_setting.by {
        FilterBy::Element(value) => {
            args.append(&mut bind_arg(value));

            if !filter_setting.exclusive {
                format!("{sql_field} = ${}", args.len())
            } else {
                format!("{sql_field} != ${}", args.len())
            }
        }
        FilterBy::Set(values_vec) => {
            let args_sql = values_vec
                .iter()
                .map(|value| {
                    args.append(&mut bind_arg(value));
                    format!("${}", args.len())
                })
                .collect::<Vec<String>>()
                .join(",");

            if !filter_setting.exclusive {
                format!("{sql_field} IN ({})", args_sql)
            } else {
                format!("{sql_field} NOT IN ({})", args_sql)
            }
        }
        FilterBy::Interval(lower_value, upper_value) => {
            let mut values: Vec<String> = Vec::new();

            match lower_value {
                LowerBound::Unbounded => (),
                LowerBound::Greater(value) => {
                    args.append(&mut bind_arg(value));
                    values.push(format!("{sql_field} > ${}", args.len()));
                }
                LowerBound::GreaterEqual(value) => {
                    args.append(&mut bind_arg(value));
                    values.push(format!("{sql_field} >= ${}", args.len()));
                }
            }

            match upper_value {
                UpperBound::Unbounded => (),
                UpperBound::Lower(value) => {
                    args.append(&mut bind_arg(value));
                    values.push(format!("{sql_field} < ${}", args.len()));
                }
                UpperBound::LowerEqual(value) => {
                    args.append(&mut bind_arg(value));
                    values.push(format!("{sql_field} <= ${}", args.len()));
                }
            }

            values.join(" AND ")
        }
        FilterBy::Contains(OperationValue::String(value)) => {
            args.push(BindArgument::String(format!("%{value}%")));

            if !filter_setting.exclusive {
                format!("{sql_field} LIKE ${}", args.len())
            } else {
                format!("{sql_field} NOT LIKE ${}", args.len())
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

/// Returns SQL to filter documents.
///
/// Since filters are the only place which can contain untrusted user values we are building the
/// SQL query with positional arguments and bind the values to them. This helps sanitization of all
/// values and prevents potential SQL injection attacks.
fn where_filter_sql(filter: &Filter, schema: &Schema) -> (String, Vec<BindArgument>) {
    let mut args: Vec<BindArgument> = Vec::new();

    let sql = filter
        .iter()
        .filter_map(|filter_setting| {
            match &filter_setting.field {
                Field::Meta(MetaField::Owner) => {
                    let filter_cmp = cmp_sql("operations_v1.public_key", filter_setting, &mut args);

                    Some(format!(
                        r#"
                        AND EXISTS (
                            SELECT
                                operations_v1.public_key
                            FROM
                                operations_v1
                            WHERE
                                operations_v1.operation_id = documents.document_id
                                AND
                                    {filter_cmp}
                        )
                        "#
                    ))
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
                    cmp_sql("documents.document_id", filter_setting, &mut args)
                )),
                Field::Meta(MetaField::DocumentViewId) => Some(format!(
                    "AND {}",
                    cmp_sql("documents.document_view_id", filter_setting, &mut args)
                )),
                Field::Field(field_name) => {
                    let field_sql = typecast_field_sql("operation_fields_v1.value", field_name, schema, true);
                    let filter_cmp = cmp_sql(&field_sql, filter_setting, &mut args);

                    Some(format!(
                        r#"
                        AND EXISTS (
                            SELECT
                                operation_fields_v1.value
                            FROM
                                document_view_fields AS document_view_fields_subquery
                                JOIN operation_fields_v1
                                    ON
                                        document_view_fields_subquery.operation_id = operation_fields_v1.operation_id
                                    AND
                                        document_view_fields_subquery.name = operation_fields_v1.name
                            WHERE
                                -- Match document_view_fields of this subquery with the parent one
                                document_view_fields.document_view_id = document_view_fields_subquery.document_view_id

                                -- Check if this document view fullfils this filter
                                AND operation_fields_v1.name = '{field_name}'
                                AND
                                    {filter_cmp}
                                AND
                                    operation_fields_v1.operation_id = document_view_fields_subquery.operation_id
                        )
                        "#
                    ))
                }
            }
        })
        .collect::<Vec<String>>()
        .join("\n");

    (sql, args)
}

/// Generate SQL for cursor-based pagination.
///
/// Read more about cursor-based pagination here:
/// https://brunoscheufler.com/blog/2022-01-01-paginating-large-ordered-datasets-with-cursor-based-pagination
///
/// ## Cursors
///
/// Our implementation follows the principle mentioned in that article, with a couple of
/// specialities due to our SQL table layout:
///
/// * We don't have auto incrementing `id` but `cursor` fields
/// * We can have duplicate, multiple document id and view id values since one row represents only
/// one document field. A document can consist of many fields. So pagination over document id's or
/// view id's is non-trivial and needs extra aid from our `cursor`
/// * Cursors _always_ need to point at the _last_ field of each document. This is assured by the
/// `convert_rows` method which returns that cursor to the client via the GraphQL response
///
/// ## Ordering
///
/// Pagination is strictly connected to the chosen ordering by the client of the results. We need
/// to take the ordering into account to understand which "next page" to show.
///
/// ## Pre-Queries
///
/// This method is async as it does some smaller "pre" SQL queries before the "main" query. This is
/// an optimization over the fact that cursors sometimes point at values which stay the same for
/// each SQL sub-SELECT, so we just do this query once and pass the values over into the "main"
/// query.
async fn where_pagination_sql(
    pool: &Pool,
    bind_args: &mut Vec<BindArgument>,
    pagination: &Pagination<PaginationCursor>,
    fields: &ApplicationFields,
    list: Option<&RelationList>,
    schema: &Schema,
    order: &Order,
) -> Result<String, DocumentStorageError> {
    // No pagination cursor was given
    if pagination.after.is_none() {
        return Ok("".to_string());
    }

    // Ignore pagination if we're in a relation list query and the cursor does not match the parent
    // document view id
    if let (Some(relation_list), Some(cursor)) = (list, pagination.after.as_ref()) {
        if Some(&relation_list.root_view_id) != cursor.root_view_id.as_ref() {
            return Ok("".to_string());
        }
    }

    // Unwrap as we know now that a cursor exists at this point
    let cursor = pagination
        .after
        .as_ref()
        .expect("Expect cursor to be set at this point");
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
        // 1. No ordering has been chosen by the client
        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        //
        // We order by default which is either:
        //
        // a) Simply paginate over document view ids, from the view where the cursor points at
        // b) If we're in a relation list, we paginate over the list index values, from where the
        // root_cursor points at
        None => match list {
            None => {
                // Collection of all documents following a certain schema id:
                //
                // -------------------------------------------------
                // document_id | view_id | field_name | ... | cursor
                // -------------------------------------------------
                // 0x01        | 0x01    | username   | ... | 0xa0
                // 0x01        | 0x01    | message    | ... | 0xc2   <--- Select
                // 0x02        | 0x02    | username   | ... | 0x06
                // 0x02        | 0x02    | message    | ... | 0x8b
                // -------------------------------------------------
                //
                // -> Select document_view_id at cursor 0xc2
                // -> Show results from document_view_id > 0x01
                let cmp_value_pre = format!(
                    r#"
                    SELECT
                        document_view_fields.document_view_id
                    FROM
                        operation_fields_v1
                        JOIN document_view_fields
                            ON operation_fields_v1.operation_id = document_view_fields.operation_id
                    WHERE
                        operation_fields_v1.cursor = '{operation_cursor}'
                    LIMIT 1
                    "#
                );

                // Make a "pre" SQL query to avoid duplicate sub SELECT's always returning the same
                // result
                let document_view_id: (String,) = query_as(&cmp_value_pre)
                    .fetch_one(pool)
                    .await
                    .map_err(|err| DocumentStorageError::FatalStorageError(err.to_string()))?;

                Ok(format!(
                    "AND documents.document_view_id > '{}'",
                    document_view_id.0
                ))
            }
            Some(_) => {
                // All documents mentioned in a root document's relation list, note that duplicates
                // are possible in relation lists:
                //
                // --------------------------------------------------||-------------------------
                // Documents from relation list                      || Relation list
                // --------------------------------------------------||-------------------------
                // document_id | view_id | field_name | ... | cursor || list_index | root_cursor
                // --------------------------------------------------||-------------------------
                // 0x03        | 0x03    | username   | ... | 0x99   || 0          | 0x54  <--- Select
                // 0x03        | 0x03    | message    | ... | 0xc2   || 0          | 0x54
                // 0x01        | 0x01    | username   | ... | 0xcd   || 1          | 0x8a
                // 0x01        | 0x01    | message    | ... | 0xea   || 1          | 0x8a
                // --------------------------------------------------||-------------------------
                //
                // -> Select list_index of relation list at root_cursor 0x54
                // -> Show results from list_index > 0
                let root_cursor = cursor
                    .root_operation_cursor
                    .as_ref()
                    .expect("Expect root_operation_cursor to be set when querying relation list");

                let cmp_value_pre = format!(
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

                // Make a "pre" SQL query to avoid duplicate sub SELECT's always returning the same
                // result
                let list_index: (i32,) = query_as(&cmp_value_pre)
                    .fetch_one(pool)
                    .await
                    .map_err(|err| DocumentStorageError::FatalStorageError(err.to_string()))?;

                // List indexes are always unique so we can simply just compare them like that
                Ok(format!(
                    "AND operation_fields_v1_list.list_index > {}",
                    list_index.0
                ))
            }
        },

        // 2. Ordering over a meta field
        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        //
        // We select the meta data from the document the cursor points at and use it to paginate
        // over.
        Some(Field::Meta(meta_field)) => {
            let (cmp_value_pre, cmp_field) = match meta_field {
                MetaField::DocumentId => {
                    // Select document_id of operation where the cursor points at
                    let cmp_value = format!(
                        r#"
                        SELECT
                            operations_v1.document_id
                        FROM
                            operation_fields_v1
                            JOIN operations_v1
                                ON operation_fields_v1.operation_id = operations_v1.operation_id
                        WHERE
                            operation_fields_v1.cursor = '{operation_cursor}'
                        LIMIT 1
                        "#
                    );

                    (cmp_value, "documents.document_id")
                }
                MetaField::DocumentViewId => {
                    // Select document_view_id of operation where the cursor points at
                    let cmp_value = format!(
                        r#"
                        SELECT
                            document_view_fields.document_view_id
                        FROM
                            operation_fields_v1
                            JOIN document_view_fields
                                ON operation_fields_v1.operation_id = document_view_fields.operation_id
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

            // Make a "pre" SQL query to avoid duplicate sub SELECT's always returning the same
            // result
            let cmp_value: (String,) = query_as(&cmp_value_pre)
                .fetch_one(pool)
                .await
                .map_err(|err| DocumentStorageError::FatalStorageError(err.to_string()))?;
            let cmp_value = format!("'{}'", cmp_value.0);

            if fields.is_empty() && list.is_none() {
                // If:
                //
                // 1. No application fields have been selected by the client
                // 2. We're not paginating over a relation list
                //
                // .. then we can do a simplification of the query, since the results were grouped
                // by documents and we can be sure that for each row the document id and view are
                // unique.
                Ok(format!("AND {cmp_field} {cmp_direction} {cmp_value}"))
            } else {
                // Cursor-based pagination
                Ok(format!(
                    r#"
                    AND (
                        {cmp_field} {cmp_direction} {cmp_value}
                        OR
                        (
                            {cmp_field} = {cmp_value}
                            AND
                                {cursor_sql}
                        )
                    )
                    "#
                ))
            }
        }

        // 3. Ordering over an application field
        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        //
        // Cursors are always pointing at the last field of a document. In the following example
        // this is the "message" field. Since we've ordered the results by "timestamp" we need to
        // manually find out what this value is by tracing back the cursor to the document to that
        // ordered field.
        //
        // Collection of all documents, ordered by "timestamp", following a certain schema id:
        //
        // -------------------------------------------------
        // document_id | view_id | field_name | ... | cursor
        // -------------------------------------------------
        // 0x01        | 0x01    | username   | ... | 0xa0
        // 0x01        | 0x01    | timestamp  | ... | 0x72 <-- Compare
        // 0x01        | 0x01    | message    | ... | 0xc2 <-- Select
        // 0x02        | 0x02    | username   | ... | 0x06
        // 0x02        | 0x02    | timestamp  | ... | 0x8f
        // 0x02        | 0x02    | message    | ... | 0x8b
        // -------------------------------------------------
        //
        // -> Select document_view_id where cursor points at
        // -> Find "timestamp" field of that document and use this value
        // -> Show results from "timestamp" value > other "timestamp" values
        Some(Field::Field(order_field_name)) => {
            // Select the value we want to compare with from the document the cursor is pointing
            // at. This is the value which we also order the whole results by
            let cmp_value_pre = format!(
                r#"
                SELECT
                    operation_fields_v1.value

                FROM
                    operation_fields_v1
                    LEFT JOIN
                        document_view_fields
                        ON document_view_fields.operation_id = operation_fields_v1.operation_id

                WHERE
                    document_view_fields.document_view_id = (
                        SELECT
                            document_view_fields.document_view_id

                        FROM
                            operation_fields_v1
                            LEFT JOIN
                                document_view_fields
                                ON document_view_fields.operation_id = operation_fields_v1.operation_id

                        WHERE
                            operation_fields_v1.cursor = '{operation_cursor}'

                        LIMIT 1
                    )
                    AND operation_fields_v1.name = '{order_field_name}'

                LIMIT 1
                "#
            );

            // Make a "pre" SQL query to avoid duplicate sub SELECT's always returning the same
            // result.
            //
            // The returned value is added to the bindable arguments array since this is untrusted
            // user content.
            let operation_fields_value: (String,) = query_as(&cmp_value_pre)
                .fetch_one(pool)
                .await
                .map_err(|err| DocumentStorageError::FatalStorageError(err.to_string()))?;
            bind_args.push(BindArgument::String(operation_fields_value.0));

            // Necessary casting for operation values of different type
            let cmp_field =
                typecast_field_sql("operation_fields_v1.value", order_field_name, schema, false);

            let bind_arg_marker = typecast_field_sql(
                &format!("${}", bind_args.len()),
                order_field_name,
                schema,
                false,
            );

            // Cursor-based pagination
            Ok(format!(
                r#"
                AND EXISTS (
                    SELECT
                        operation_fields_v1.value
                    FROM
                        operation_fields_v1
                        LEFT JOIN document_view_fields
                            ON operation_fields_v1.operation_id = document_view_fields.operation_id
                    WHERE
                        operation_fields_v1.name = '{order_field_name}'
                        AND
                            document_view_fields.document_view_id = documents.document_view_id
                        AND
                            (
                                {cmp_field} {cmp_direction} {bind_arg_marker}
                                OR
                                (
                                    {cmp_field} = {bind_arg_marker}
                                    AND
                                        {cursor_sql}
                                )
                            )
                )
                "#
            ))
        }
    }
}

fn order_sql(
    order: &Order,
    schema: &Schema,
    list: Option<&RelationList>,
    fields: &ApplicationFields,
) -> String {
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

    // Order by document view id, except of if it was selected manually. We need this to assemble
    // the rows to documents correctly at the end
    let id_sql = {
        if fields.len() > 1 {
            match order.field {
                Some(Field::Meta(MetaField::DocumentViewId)) => None,
                _ => Some("documents.document_view_id ASC".to_string()),
            }
        } else {
            // Skip this step when only _one_ field was selected for this query to not mess with
            // pagination.
            //
            // We're relying on the "cursor" being the tie-breaker for duplicates. If we would
            // still order by document view id, this would become undesirably our tie-breaker
            // instead - as every field is automatically another unique document when only one was
            // selected hence we will never order by cursor on top of that.
            //
            // When no field was selected we just skip this ordering to simplify the query.
            None
        }
    };

    // On top we sort always by the unique operation cursor in case the previous order value is
    // equal between two rows
    let cursor_sql = match list {
        Some(_) => Some("operation_fields_v1_list.cursor ASC".to_string()),
        None => Some("operation_fields_v1.cursor ASC".to_string()),
    };

    let order = concatenate_sql(&[custom, list_sql, id_sql, cursor_sql]);

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
            let root_view_id = &relation_list.root_view_id;
            let field_type = match relation_list.list_type {
                RelationListType::Pinned => "pinned_relation_list",
                RelationListType::Unpinned => "relation_list",
            };

            format!(
                r#"
                document_view_fields_list.document_view_id = '{root_view_id}'
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
        let (and_filters, mut bind_args) = where_filter_sql(&args.filter, schema);
        let and_pagination = where_pagination_sql(
            &self.pool,
            &mut bind_args,
            &args.pagination,
            &application_fields,
            list,
            schema,
            &args.order,
        )
        .await?;

        let order = order_sql(&args.order, schema, list, &application_fields);
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

        let mut query = query_as::<_, QueryRow>(&sea_quel);

        // Bind untrusted user arguments to query
        query = bind_to_query(query, &bind_args);

        let mut rows: Vec<QueryRow> = query
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
        let (and_filters, bind_args) = where_filter_sql(&args.filter, schema);

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

        let mut query = query_as::<_, (i64,)>(&count_sql);

        // Bind untrusted user arguments to query
        query = bind_to_query(query, &bind_args);

        let result: Option<(i64,)> = query
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
/// need to merge multiple rows / operation fields into one document.
///
/// This method also returns a cursor for each document to the clients which can then use it to
/// control pagination. Since every cursor is unique for each operation field and there might be
/// multiple cursors for one document we need to make sure to _always_ pick the _last_ cursor for
/// each document.
///
/// ```text
/// -------------------------------------------------
/// document_id | view_id | field_name | ... | cursor
/// -------------------------------------------------
/// 0x01        | 0x01    | username   | ... | 0xa0
/// 0x01        | 0x01    | message    | ... | 0xc2   <--- Last Cursor
/// 0x02        | 0x02    | username   | ... | 0x06
/// 0x02        | 0x02    | message    | ... | 0x8b   <--- Last Cursor
/// 0x04        | 0x04    | username   | ... | 0x06
/// 0x04        | 0x04    | message    | ... | 0x8b   <--- Last Cursor
/// -------------------------------------------------
/// ```
///
fn convert_rows(
    rows: Vec<QueryRow>,
    list: Option<&RelationList>,
    fields: &ApplicationFields,
    schema_id: &SchemaId,
) -> Vec<(PaginationCursor, StorageDocument)> {
    let mut converted: Vec<(PaginationCursor, StorageDocument)> = Vec::new();

    if rows.is_empty() {
        return converted;
    }

    // Helper method to convert database row into final document and cursor type
    let finalize_document = |row: &QueryRow,
                             collected_fields: Vec<DocumentViewFieldRow>,
                             collected_rows: &HashMap<FieldName, QueryRow>|
     -> (PaginationCursor, StorageDocument) {
        // Determine cursor for this document by looking at the last field
        let cursor = {
            let last_field = collected_fields
                .last()
                .expect("Needs to be at least one field");

            row_to_cursor(
                collected_rows
                    .get(&last_field.name)
                    .expect("Field selected for ordering needs to be inside of document"),
                list,
            )
        };

        // Convert all gathered data into final `StorageDocument` format
        let fields = parse_document_view_field_rows(collected_fields);

        let document = StorageDocument {
            id: row.document_id.parse().unwrap(),
            fields: Some(fields),
            schema_id: schema_id.clone(),
            view_id: row.document_view_id.parse().unwrap(),
            author: (&row.owner).into(),
            deleted: row.is_deleted,
        };

        (cursor, document)
    };

    let rows_per_document = std::cmp::max(fields.len(), 1);

    let mut current = rows[0].clone();
    let mut current_fields = Vec::with_capacity(rows_per_document);
    let mut current_rows = HashMap::new();

    for (index, row) in rows.into_iter().enumerate() {
        // We observed a new document coming up in the next row, time to change
        if index % rows_per_document == 0 && index > 0 {
            // Finalize the current document, convert it and push it into the final array
            let (cursor, document) = finalize_document(&current, current_fields, &current_rows);
            converted.push((cursor, document));

            // Change the pointer to the next document
            current = row.clone();
            current_fields = Vec::with_capacity(rows_per_document);
        }

        // Collect original rows from SQL query
        current_rows.insert(row.name.clone(), row.clone());

        // Collect every field of the document to assemble it later into a `StorageDocument`
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
    let (cursor, document) = finalize_document(&current, current_fields, &current_rows);
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
                Some(relation_list.root_view_id.clone()),
            )
        }
        None => PaginationCursor::new(row.cmp_value_cursor.as_str().into(), None, None),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use p2panda_rs::document::traits::AsDocument;
    use p2panda_rs::document::DocumentViewId;
    use p2panda_rs::hash::Hash;
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::operation::{OperationValue, PinnedRelationList};
    use p2panda_rs::schema::{FieldType, Schema, SchemaId};
    use p2panda_rs::storage_provider::traits::DocumentStore;
    use p2panda_rs::test_utils::fixtures::{key_pair, schema_id};
    use rstest::rstest;

    use crate::db::models::{OptionalOwner, QueryRow};
    use crate::db::query::{
        Direction, Field, Filter, MetaField, Order, Pagination, PaginationField, Select,
    };
    use crate::db::stores::{OperationCursor, RelationList};
    use crate::db::types::StorageDocument;
    use crate::test_utils::{
        add_document, add_schema, add_schema_and_documents, doggo_fields, doggo_schema,
        populate_and_materialize, populate_store_config, test_runner, PopulateStoreConfig,
        TestNode,
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

    async fn create_chat_test_data(
        node: &mut TestNode,
        key_pair: &KeyPair,
    ) -> (Schema, Vec<DocumentViewId>) {
        add_schema_and_documents(
            node,
            "chat",
            vec![
                vec![
                    ("message", "Hello, Panda!".into(), None),
                    ("username", "penguin".into(), None),
                    ("timestamp", 1687265969.into(), None),
                ],
                vec![
                    ("message", "Oh, howdy, Pengi!".into(), None),
                    ("username", "panda".into(), None),
                    ("timestamp", 1687266014.into(), None),
                ],
                vec![
                    ("message", "How are you?".into(), None),
                    ("username", "panda".into(), None),
                    ("timestamp", 1687266032.into(), None),
                ],
                vec![
                    ("message", "I miss Pengolina. How about you?".into(), None),
                    ("username", "penguin".into(), None),
                    ("timestamp", 1687266055.into(), None),
                ],
                vec![
                    ("message", "I am cute and very hungry".into(), None),
                    ("username", "panda".into(), None),
                    ("timestamp", 1687266141.into(), None),
                ],
                vec![
                    ("message", "(°◇°) !!".into(), None),
                    ("username", "penguin".into(), None),
                    ("timestamp", 1687266160.into(), None),
                ],
            ],
            key_pair,
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
            let (schema, _) = create_events_test_data(&mut node, &key_pair).await;

            let (_, documents) = node
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
    #[case::order_by_timestamp(
        Order::new(&"timestamp".into(), &Direction::Ascending),
        "message".into(),
        vec![
            "Hello, Panda!".into(),
            "Oh, howdy, Pengi!".into(),
            "How are you?".into(),
            "I miss Pengolina. How about you?".into(),
            "I am cute and very hungry".into(),
            "(°◇°) !!".into(),
        ],
    )]
    #[case::order_by_timestamp_descending(
        Order::new(&"timestamp".into(), &Direction::Descending),
        "message".into(),
        vec![
            "(°◇°) !!".into(),
            "I am cute and very hungry".into(),
            "I miss Pengolina. How about you?".into(),
            "How are you?".into(),
            "Oh, howdy, Pengi!".into(),
            "Hello, Panda!".into(),
        ],
    )]
    #[case::order_by_message(
        Order::new(&"message".into(), &Direction::Ascending),
        "message".into(),
        vec![
            "(°◇°) !!".into(),
            "Hello, Panda!".into(),
            "How are you?".into(),
            "I am cute and very hungry".into(),
            "I miss Pengolina. How about you?".into(),
            "Oh, howdy, Pengi!".into(),
        ],
    )]
    fn pagination_over_ordered_fields(
        key_pair: KeyPair,
        #[case] order: Order,
        #[case] selected_field: String,
        #[case] expected_fields: Vec<OperationValue>,
    ) {
        test_runner(|mut node: TestNode| async move {
            let (schema, view_ids) = create_chat_test_data(&mut node, &key_pair).await;

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
                &Select::new(&[
                    Field::Meta(MetaField::DocumentViewId),
                    Field::Field("message".into()),
                    Field::Field("username".into()),
                    Field::Field("timestamp".into()),
                ]),
                &Filter::default(),
                &order,
            );

            // Go through all pages, one document at a time
            for (index, expected_field) in expected_fields.into_iter().enumerate() {
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

                if view_ids.len() - 1 == index {
                    assert_eq!(pagination_data.has_next_page, false);
                } else {
                    assert_eq!(pagination_data.has_next_page, true);
                }

                assert_eq!(pagination_data.total_count, Some(view_ids.len() as u64));
                assert_eq!(documents.len(), 1);
                assert_eq!(documents[0].1.get(&selected_field), Some(&expected_field));
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

            assert_eq!(pagination_data.total_count, Some(view_ids.len() as u64));
            assert_eq!(pagination_data.end_cursor, None);
            assert_eq!(pagination_data.has_next_page, false);
            assert_eq!(documents.len(), 0);
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
            let (venues_schema, venues_view_ids) =
                create_venues_test_data(&mut node, &key_pair).await;

            let (_, visited_view_ids) = create_visited_test_data(
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
            let (venues_schema, _) = create_venues_test_data(&mut node, &key_pair).await;

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

            let (_, documents) = node
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

            let (_, documents) = node
                .context
                .store
                .query(&venues_schema, &args, Some(&list))
                .await
                .expect("Query failed");

            assert!(documents.is_empty());

            // Query selecting application field
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

            node.context
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

            let (_, visited_view_ids) = create_visited_test_data(
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
            let (venues_schema, venues_view_ids) =
                create_venues_test_data(&mut node, &key_pair).await;

            let (_, visited_view_ids) = create_visited_test_data(
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
            let (venues_schema, _) = create_venues_test_data(&mut node, &key_pair).await;

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
            let (venues_schema, venues_view_ids) =
                create_venues_test_data(&mut node, &key_pair).await;

            let (_, visited_view_ids) = create_visited_test_data(
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
            let (venues_schema, venues_view_ids) =
                create_venues_test_data(&mut node, &key_pair).await;

            let (visited_schema, _) = create_visited_test_data(
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
        let cursor_3 = Hash::new_from_bytes(&[0, 5]).to_string();
        let cursor_4 = Hash::new_from_bytes(&[0, 6]).to_string();

        let query_rows = vec![
            // First document
            // ==============
            QueryRow {
                document_id: first_document_hash.clone(),
                document_view_id: first_document_hash.clone(),
                operation_id: first_document_hash.clone(),
                is_deleted: false,
                is_edited: false,
                // This is the "root" cursor, marking the position of the document inside a
                // relation list
                root_cursor: root_cursor_1.clone(),
                // This is the "field" cursor, marking the value we're using to paginate the
                // resulting documents with.
                //
                // Cursors are unique for each operation field.
                cmp_value_cursor: cursor_1.clone(), // Cursor #1
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
                cmp_value_cursor: cursor_2.clone(), // Cursor #2
                owner: OptionalOwner::default(),
                name: "is_admin".to_string(),
                value: Some("false".to_string()),
                field_type: "bool".to_string(),
                list_index: 0,
            },
            // Second document
            // ===============
            QueryRow {
                document_id: second_document_hash.clone(),
                document_view_id: second_document_hash.clone(),
                operation_id: second_document_hash.clone(),
                is_deleted: false,
                is_edited: false,
                root_cursor: root_cursor_2.clone(),
                cmp_value_cursor: cursor_3.clone(), // Cursor #3
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
                cmp_value_cursor: cursor_4.clone(), // Cursor #4
                owner: OptionalOwner::default(),
                name: "is_admin".to_string(),
                value: Some("true".to_string()),
                field_type: "bool".to_string(),
                list_index: 0,
            },
        ];

        // 1.
        //
        // Convert query rows into documents as if this is a relation list query. We do this by
        // passing in the relation list information (the "root") from where this query was executed
        // from
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

        // We expect the cursor of the last query row to be returned per document, that is cursor
        // #2 and #4
        assert_eq!(
            result[0].0,
            PaginationCursor::new(
                OperationCursor::from(cursor_2.as_str()),
                Some(OperationCursor::from(root_cursor_1.as_str())),
                Some(relation_list_hash.parse().unwrap())
            )
        );
        assert_eq!(
            result[1].0,
            PaginationCursor::new(
                OperationCursor::from(cursor_4.as_str()),
                Some(OperationCursor::from(root_cursor_2.as_str())),
                Some(relation_list_hash.parse().unwrap())
            )
        );

        // 2.
        //
        // We pretend now that this query was executed without a relation list
        let result = convert_rows(
            query_rows,
            None,
            &vec!["username".to_string(), "is_admin".to_string()],
            &schema_id,
        );

        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0].0,
            PaginationCursor::new(OperationCursor::from(cursor_2.as_str()), None, None)
        );
        assert_eq!(
            result[1].0,
            PaginationCursor::new(OperationCursor::from(cursor_4.as_str()), None, None)
        );
    }

    #[rstest]
    fn query_updated_documents_with_filter(
        #[from(populate_store_config)]
        // This config will populate the store with 10 documents which each have their username
        // field updated from "bubu" (doggo_schema) to "me"
        #[with(2, 10, vec![KeyPair::new()], false, doggo_schema(), doggo_fields(),
               vec![("username", OperationValue::String("me".to_string()))]
        )]
        config: PopulateStoreConfig,
    ) {
        test_runner(|mut node: TestNode| async move {
            // Populate the store and materialize all documents.
            populate_and_materialize(&mut node, &config).await;

            let schema = doggo_schema();

            let documents = node
                .context
                .store
                .get_documents_by_schema(schema.id())
                .await
                .unwrap();
            assert_eq!(documents.len(), 10);
            for document in documents {
                if document.get("username").unwrap() != &OperationValue::String("me".into()) {
                    panic!("All 'username' fields should have been updated to 'me'");
                }
            }

            let args = Query::new(
                &Pagination::new(
                    &NonZeroU64::new(5).unwrap(),
                    None,
                    &vec![PaginationField::TotalCount],
                ),
                &Select::new(&[
                    Field::Meta(MetaField::DocumentId),
                    Field::Field("username".into()),
                    Field::Field("height".into()),
                    Field::Field("age".into()),
                    Field::Field("is_admin".into()),
                ]),
                &Filter::default().fields(&[("username", &["me".into()])]),
                &Order::default(),
            );

            let (_pagination_data, documents) = node
                .context
                .store
                .query(&schema, &args, None)
                .await
                .expect("Query failed");

            // We expect 5 documents and each should contain 4 fields.
            assert_eq!(documents.len(), 5);
            for (_cursor, document) in documents {
                assert_eq!(document.fields().unwrap().len(), 4);
                assert!(document.is_edited());
            }
        });
    }
}
