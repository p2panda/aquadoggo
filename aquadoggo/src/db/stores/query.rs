// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module offers a query API to find many p2panda documents, filtered or sorted by custom
//! parameters. The results are batched via cursor-based pagination.
use std::collections::HashMap;

use p2panda_rs::document::DocumentViewFields;
use p2panda_rs::identity::PublicKey;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::{Schema, SchemaId};
use p2panda_rs::storage_provider::error::DocumentStorageError;
use sqlx::any::AnyRow;
use sqlx::{query_as, FromRow};

use crate::db::models::utils::parse_document_view_field_rows;
use crate::db::models::{DocumentViewFieldRow, QueryRow};
use crate::db::query::{
    Cursor, Direction, Field, Filter, FilterBy, FilterSetting, LowerBound, MetaField, Order,
    Pagination, Select, UpperBound,
};
use crate::db::types::StorageDocument;
use crate::db::SqlStore;

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
        _ => field_name.to_string(),
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
            format!("{sql_field} LIKE '%{value}%'")
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
                                operation_fields_v1.operation_id = document_view_fields.operation_id
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

fn pagination_sql<C>(pagination: &Pagination<C>, order: &Order) -> String
where
    C: Cursor,
{
    match &pagination.after {
        Some(cursor) => {
            let cursor_str = cursor.encode();

            match &order.field {
                Field::Meta(MetaField::DocumentViewId) => {
                    format!("AND documents.document_view_id > '{cursor_str}'")
                }
                Field::Meta(MetaField::DocumentId) => {
                    format!("AND documents.document_id > '{cursor_str}'")
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
                                        WHERE
                                            operation_fields_v1.name = '{order_field_name}'
                                            AND
                                                documents.document_id > '{cursor_str}'
                                    )
                                )
                                OR
                                (
                                    operation_fields_v1.value = (
                                        SELECT
                                            operation_fields_v1.value
                                        FROM
                                            operation_fields_v1
                                        WHERE
                                            operation_fields_v1.name = '{order_field_name}'
                                            AND
                                                documents.document_id > '{cursor_str}'
                                    )
                                    AND
                                        documents.document_id > '{order_field_name}'
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
                    WHERE
                        operation_fields_v1.operation_id = document_view_fields.operation_id
                        AND
                            operation_fields_v1.name = '{}'
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

fn limit_sql<C>(pagination: &Pagination<C>, fields: &Vec<String>) -> String
where
    C: Cursor,
{
    // We multiply the value by the number of fields we selected. If no fields have
    // been selected we just take the page size as is
    let page_size = pagination.first.get() * std::cmp::max(1, fields.len() as u64);

    // ... and add + 1 for the "has next page" flag
    format!("LIMIT {page_size} + 1")
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

impl SqlStore {
    pub async fn query<C: Cursor>(
        &self,
        schema: &Schema,
        args: &Query<C>,
    ) -> Result<Vec<StorageDocument>, DocumentStorageError> {
        let schema_id = schema.id();

        // Get all selected application fields from query
        let select_fields: Vec<String> = args
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
        let and_select = application_select_sql(&select_fields);
        let and_filters = filter_sql(&args.filter);
        let and_pagination = pagination_sql(&args.pagination, &args.order);

        let group = group_sql(&select_fields);
        let order = order_sql(&args.order, schema);
        let limit = limit_sql(&args.pagination, &select_fields);

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
                document_view_fields.document_view_id,
                document_view_fields.operation_id,

                -- If a document got deleted we already store in the database, the edited state we
                -- find out by checking if there is more operations next to the initial "create"
                -- operation
                documents.is_deleted,
                EXISTS (
                    SELECT
                        operations_v1.public_key
                    FROM
                        operations_v1
                    WHERE
                        operations_v1.action != "create"
                        AND
                            operations_v1.document_id = documents.document_id
                    LIMIT 1
                ) AS is_edited,

                -- The original owner of a document we get by checking which public key signed the
                -- "create" operation, the hash of that operation is the same as the document id
                (
                    SELECT
                        operations_v1.public_key
                    FROM
                        operations_v1
                    WHERE
                        operations_v1.operation_id = documents.document_id
                ) AS owner,

                -- Finally we get the application data by selecting the name and value field. We
                -- need the field type and list index to understand what the type of the data is and
                -- if it is a relation, in what order the entries occur
                operation_fields_v1.name,
                operation_fields_v1.value,
                operation_fields_v1.field_type,
                operation_fields_v1.list_index

            FROM
                documents
                LEFT JOIN document_view_fields
                    ON
                        document_view_fields.document_view_id = documents.document_view_id
                LEFT JOIN operation_fields_v1
                    ON
                        operation_fields_v1.operation_id = document_view_fields.operation_id
                    AND
                        operation_fields_v1.name = document_view_fields.name

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

        let rows: Vec<QueryRow> = query_as::<_, QueryRow>(&sea_quel)
            .fetch_all(&self.pool)
            .await
            .map_err(|err| DocumentStorageError::FatalStorageError(err.to_string()))?;

        let documents = {
            let mut view_map: HashMap<String, Vec<QueryRow>> = HashMap::new();

            for row in rows {
                match view_map.get_mut(&row.document_id) {
                    Some(field_vec) => {
                        field_vec.push(row);
                    }
                    None => {
                        view_map.insert(row.document_id.clone(), vec![row.clone()]);
                    }
                }
            }

            view_map
                .into_values()
                .map(|rows| {
                    // Unwrap conversions as we trust that values from database have been checked
                    let id = rows[0].document_id.parse().unwrap();
                    let view_id = rows[0].document_view_id.parse().unwrap();
                    let author = rows[0].owner.parse().unwrap();
                    let deleted = rows[0].is_deleted;

                    let document_field_rows: Vec<DocumentViewFieldRow> = rows
                        .into_iter()
                        .map(|query_row| DocumentViewFieldRow {
                            document_id: query_row.document_id,
                            document_view_id: query_row.document_view_id,
                            operation_id: query_row.operation_id,
                            name: query_row.name,
                            list_index: query_row.list_index,
                            field_type: query_row.field_type,
                            value: query_row.value,
                        })
                        .collect();

                    let fields = parse_document_view_field_rows(document_field_rows);

                    StorageDocument {
                        id,
                        fields: Some(fields),
                        schema_id: schema.id().clone(),
                        view_id,
                        author,
                        deleted,
                    }
                })
                .collect()
        };

        Ok(documents)
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
        let pagination =
            Pagination::<String>::new(&NonZeroU64::new(50).unwrap(), Some(&"cursor".into()));

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
