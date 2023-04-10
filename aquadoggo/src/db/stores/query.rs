// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module offers a query API to find many p2panda documents, filtered or sorted by custom
//! parameters. The results are batched via cursor-based pagination.
use std::collections::HashMap;
use std::str::FromStr;

use anyhow::bail;
use p2panda_rs::document::{DocumentId, DocumentViewFields};
use p2panda_rs::identity::PublicKey;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::{Schema, SchemaId};
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

pub type QueryResponse = (
    PaginationData<DocumentCursor>,
    Vec<(DocumentCursor, StorageDocument)>,
);

#[derive(Debug, Clone)]
pub struct DocumentCursor {
    pub list_index: u64,
    pub document_id: DocumentId,
}

impl DocumentCursor {
    pub fn new(list_index: u64, document_id: DocumentId) -> Self {
        Self {
            list_index,
            document_id,
        }
    }
}

impl From<&QueryRow> for DocumentCursor {
    fn from(row: &QueryRow) -> Self {
        let document_id = row.document_id.parse().unwrap();
        Self::new(row.list_index as u64, document_id)
    }
}

impl Cursor for DocumentCursor {
    type Error = anyhow::Error;

    fn decode(value: &str) -> Result<Self, Self::Error> {
        // @TODO: Decode base64 string
        let parts: Vec<&str> = value.split('_').collect();

        if parts.len() != 2 {
            bail!("Invalid amount of cursor parts");
        }

        Ok(Self::new(
            u64::from_str(parts[0])?,
            DocumentId::from_str(parts[1])?,
        ))
    }

    fn encode(&self) -> String {
        // @TODO: Generate base64 string
        format!("{}_{}", self.list_index, self.document_id)
    }
}

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
            // @TODO: Write a test for this to check if this correctly
            // "flattens" relation lists
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
                        )
                        "#
                    ))
                }
            }
        })
        .collect::<Vec<String>>()
        .join("\n")
}

fn pagination_sql(pagination: &Pagination<DocumentCursor>, order: &Order) -> String {
    match &pagination.after {
        Some(cursor) => {
            let cursor_str = cursor.document_id.to_string();

            match &order.field {
                Field::Meta(MetaField::DocumentId) => {
                    format!("AND documents.document_id > '{cursor_str}'")
                }
                Field::Meta(MetaField::DocumentViewId) => {
                    // @TODO
                    todo!("Not implemented yet");
                }
                Field::Meta(MetaField::Owner) => {
                    // @TODO
                    todo!("Not implemented yet");
                }
                Field::Meta(MetaField::Edited) => {
                    // @TODO
                    todo!("Not implemented yet");
                }
                Field::Meta(MetaField::Deleted) => {
                    // @TODO
                    todo!("not implemented yet");
                }
                Field::Field(order_field_name) => {
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
                                    operation_fields_v1.value > (
                                        SELECT
                                            operation_fields_v1.value
                                        FROM
                                            operation_fields_v1
                                        LEFT JOIN
                                            operations_v1
                                            ON
                                                operations_v1.operation_id = operation_fields_v1.operation_id
                                        WHERE
                                            operation_fields_v1.name = '{order_field_name}'
                                            AND
                                                operations_v1.document_id = '{cursor_str}'
                                    )
                                    OR
                                    (
                                        operation_fields_v1.value = (
                                            SELECT
                                                operation_fields_v1.value
                                            FROM
                                                operation_fields_v1
                                            LEFT JOIN
                                                operations_v1
                                                ON
                                                    operations_v1.operation_id = operation_fields_v1.operation_id
                                            WHERE
                                                operation_fields_v1.name = '{order_field_name}'
                                                AND
                                                    operations_v1.document_id = '{cursor_str}'
                                        )
                                        AND
                                            documents.document_id > '{cursor_str}'
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

fn group_sql(fields: &Vec<String>) -> String {
    // When no field was selected we can group the results by document id, this allows us to
    // paginate the results easily
    if fields.is_empty() {
        "GROUP BY documents.document_id".to_string()
    } else {
        "".to_string()
    }
}

fn order_sql(order: &Order, schema: &Schema) -> String {
    let direction = match order.direction {
        Direction::Ascending => "ASC",
        Direction::Descending => "DESC",
    };

    match &order.field {
        Field::Meta(MetaField::DocumentId) => {
            format!("ORDER BY documents.document_id {direction}")
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
                    documents.document_id ASC,

                    -- Lastly we should order them by list index, which preserves
                    -- the ordering of (pinned) relation list values
                    operation_fields_v1.list_index ASC
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

                    -- On top we sort always by id in case the previous order
                    -- value is equal between two rows
                    documents.document_id ASC,

                    -- Lastly we should order them by list index, which preserves
                    -- the ordering of (pinned) relation list values
                    operation_fields_v1.list_index ASC
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
    // We multiply the value by the number of fields we selected. If no fields have
    // been selected we just take the page size as is
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

fn total_count_sql(schema_id: &SchemaId, filter: &Filter) -> String {
    let and_filters = filter_sql(&filter);

    format!(
        r#"
            SELECT
                COUNT(documents.document_id)

            FROM
                documents
                INNER JOIN document_view_fields
                    ON documents.document_view_id = document_view_fields.document_view_id
                INNER JOIN operation_fields_v1
                    ON
                        document_view_fields.operation_id = operation_fields_v1.operation_id
                        AND
                            document_view_fields.name = operation_fields_v1.name

            WHERE
                documents.schema_id = '{schema_id}'
                {and_filters}

            -- Group application fields by name to make sure we get actual number of documents
            GROUP BY operation_fields_v1.name
        "#
    )
}

fn edited_sql(select: &Select) -> String {
    if select.fields.contains(&Field::Meta(MetaField::Edited)) {
        format!(
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
        )
    } else {
        // Use constant when we do not query for edited status. This is useful for keeping the
        // resulting row type in shape, otherwise we would need to dynamically change it depending
        // on what meta fields were selected.
        "0".to_string()
    }
}

fn owner_sql(select: &Select) -> String {
    if select.fields.contains(&Field::Meta(MetaField::Owner)) {
        format!(
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
        )
    } else {
        // Use constant when we do not query owner. This is useful for keeping the resulting row
        // type in shape, otherwise we would need to dynamically change it depending on what meta
        // fields were selected.
        //
        // The value is so funny to make sure it passes ed25519 public key validation.
        "'0000000000000000000000000000000000000000000000000000000000000000'".to_string()
    }
}

impl SqlStore {
    pub async fn query(
        &self,
        schema: &Schema,
        args: &Query<DocumentCursor>,
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
        let and_select = application_select_sql(&application_fields);
        let and_filters = filter_sql(&args.filter);
        let and_pagination = pagination_sql(&args.pagination, &args.order);

        let select_edited = edited_sql(&args.select);
        let select_owner = owner_sql(&args.select);

        let group = group_sql(&application_fields);
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
                -- We get all the meta informations first, let's start with the document id and
                -- document view id, schema id and id of the operation which holds the data
                documents.document_id,
                documents.document_view_id,
                operation_fields_v1.operation_id,

                -- If a document got deleted we already store in the database, let's get it in any
                -- case ince we get it for free
                documents.is_deleted,

                -- Optionally we check for the edited status and owner of the document
                {select_edited} AS is_edited,
                {select_owner} AS owner,

                -- Finally we get the application data by selecting the name and value field. We
                -- need the field type and list index to understand what the type of the data is and
                -- if it is a relation, in what order the entries occur
                operation_fields_v1.name,
                operation_fields_v1.value,
                operation_fields_v1.field_type,
                operation_fields_v1.list_index

            FROM
                documents
                INNER JOIN document_view_fields
                    ON documents.document_view_id = document_view_fields.document_view_id
                INNER JOIN operation_fields_v1
                    ON
                        document_view_fields.operation_id = operation_fields_v1.operation_id
                        AND
                            document_view_fields.name = operation_fields_v1.name

            WHERE
                -- We always filter by the queried schema of that collection
                documents.schema_id = '{schema_id}'

                -- .. and select only the operation fields we're interested in
                {and_select}

                -- .. and further filter the data by custom parameters
                {and_filters}

                -- Lastly we batch all results into smaller chunks via cursor pagination
                {and_pagination}

            -- In case we did not select any fields of interest we can group the results by
            -- document id, since we're only interested in the meta data we get from simply SELECTing
            -- them per row
            {group}

            -- We always order the rows by document id and list_index, but there might also be
            -- user-defined ordering on top of that
            {order}

            -- Connected to cursor pagination we limit the number of rows
            {limit}
        "#
        );

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
            let result: (i32,) = query_as(&total_count_sql(&schema_id, &args.filter))
                .fetch_one(&self.pool)
                .await
                .map_err(|err| DocumentStorageError::FatalStorageError(err.to_string()))?;

            Some(result.0 as u64)
        } else {
            None
        };

        // Determine cursors for pagination by looking at beginning and end of results
        let start_cursor = rows.first().map(DocumentCursor::from);
        let end_cursor = rows.last().map(DocumentCursor::from);

        let pagination_data = PaginationData {
            total_count,
            has_next_page,
            // @TODO: Implement backwards pagination
            has_previous_page: false,
            start_cursor,
            end_cursor,
        };

        // Finally convert everything into the right format
        let documents = Self::convert_rows(rows, schema.id());

        Ok((pagination_data, documents))
    }

    /// Merges all fields from the database into documents.
    fn convert_rows(
        rows: Vec<QueryRow>,
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

                let cursor: DocumentCursor = row.into();

                (cursor, document)
            };

            let last_row = rows.last().unwrap().clone();

            let mut current = rows[0].clone();
            let mut current_fields = Vec::new();

            for row in rows {
                // We observed a new document coming up in the next row, time to change
                if current.document_id != row.document_id {
                    // Finalize the current document, convert it and push it into the final
                    // array
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
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use crate::db::query::{Direction, Field, Filter, MetaField, Order, Pagination};

    use super::Query;

    #[test]
    fn create_find_many_query() {
        let order = Order::new(&Field::Meta(MetaField::Owner), &Direction::Descending);
        let pagination = Pagination::<String>::new(
            &NonZeroU64::new(50).unwrap(),
            Some(&"cursor".into()),
            &vec![],
        );

        let query = Query {
            order: order.clone(),
            pagination: pagination.clone(),
            ..Query::default()
        };

        assert_eq!(query.pagination, pagination);
        assert_eq!(query.order, order);
        assert_eq!(query.filter, Filter::default());
    }
}
