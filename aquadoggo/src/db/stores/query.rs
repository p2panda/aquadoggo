// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module offers a query API to find one or many p2panda documents, filtered or sorted by
//! custom parameters. Multiple results are batched via cursor-based pagination.
use std::cmp::min;

use p2panda_rs::document::DocumentViewFields;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::schema::{Schema, SchemaId};
use p2panda_rs::storage_provider::error::DocumentStorageError;
use sqlx::{query_as, FromRow};

use crate::db::query::{
    Cursor, Direction, Field, Filter, FilterBy, FilterSetting, LowerBound, MetaField, Order,
    Pagination, Select, UpperBound,
};
use crate::db::SqlStore;

#[derive(FromRow, Debug, Clone)]
pub struct QueryRow {
    pub name: String,
    pub value: String,
    pub document_view_id: String,
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

/// Helper method to determine the field type of the given field by looking at the schema and
/// derive a SQL type cast function from it.
fn typecast_field(sql_field: &str, field_name: &str, schema: &Schema) -> String {
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

fn value_str(value: &OperationValue) -> String {
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

fn filter_by_str(sql_field: &str, filter_setting: &FilterSetting) -> String {
    match &filter_setting.by {
        FilterBy::Element(value) => {
            if !filter_setting.exclusive {
                format!("{sql_field} = {}", value_str(value))
            } else {
                format!("{sql_field} != {}", value_str(value))
            }
        }
        FilterBy::Set(values_vec) => {
            // @TODO: Write a test for this to check if this correctly
            // "flattens" relation lists
            let value_sql = values_vec
                .iter()
                .map(value_str)
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
                    values.push(format!("{sql_field} > {}", value_str(value)));
                }
                LowerBound::GreaterEqual(value) => {
                    values.push(format!("{sql_field} >= {}", value_str(value)));
                }
            }

            match upper_value {
                UpperBound::Unbounded => (),
                UpperBound::Lower(value) => {
                    values.push(format!("{sql_field} < {}", value_str(value)));
                }
                UpperBound::LowerEqual(value) => {
                    values.push(format!("{sql_field} <= {}", value_str(value)));
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

impl SqlStore {
    pub async fn query<C: Cursor>(
        &self,
        schema: Schema,
        args: &Query<C>,
    ) -> Result<Vec<DocumentViewFields>, DocumentStorageError> {
        let schema_id = schema.id();

        let and_filters = args
            .filter
            .iter()
            .filter_map(|filter_setting| {
                match &filter_setting.field {
                    Field::Meta(MetaField::Edited) => {
                        // @TODO: This is not supported yet
                        None
                    }
                    Field::Meta(MetaField::Owner) => {
                        // @TODO: This is not supported yet
                        None
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
                    Field::Meta(MetaField::DocumentId) => {
                        Some(format!("AND {}", filter_by_str("documents.document_id", filter_setting)))
                    }
                    Field::Meta(MetaField::DocumentViewId) => {
                        Some(format!("AND {}", filter_by_str("documents.document_view_id", filter_setting)))
                    }
                    Field::Field(field_name) => {
                        let filter_cmp = filter_by_str("operation_fields_v1.value", filter_setting);

                        Some(format!(
                            r#"
                                AND
                                    EXISTS (
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
            .join("\n");

        // Prepare SQL to only select only specific document fields (default is to select no fields)
        let select_fields_vec: Vec<String> = args
            .select
            .iter()
            .filter_map(|field| {
                if let Field::Field(field_name) = field {
                    Some(field_name.clone())
                } else {
                    None
                }
            })
            .collect();

        let and_application_fields = if select_fields_vec.is_empty() {
            "".to_string()
        } else {
            format!(
                "AND operation_fields_v1.name IN ({})",
                select_fields_vec.join(", ")
            )
        };

        // When no field was selected we can group the results by document id, this allows us to
        // paginate the results easily
        let group = if select_fields_vec.is_empty() {
            "GROUP BY documents.document_id"
        } else {
            ""
        };

        let and_pagination = match &args.pagination.after {
            Some(cursor) => {
                let cursor_str = cursor.encode();

                match &args.order.field {
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
                                AND
                                    EXISTS (
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
        };

        let direction = match args.order.direction {
            Direction::Ascending => "ASC",
            Direction::Descending => "DESC",
        };

        let order = match &args.order.field {
            Field::Meta(MetaField::DocumentViewId) => {
                "ORDER BY documents.document_view_id {direction}".to_string()
            }
            Field::Meta(MetaField::DocumentId) => {
                "ORDER BY documents.document_id {direction}".to_string()
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
                            documents.document_id ASC
                    "#,
                    typecast_field("value", field_name, &schema),
                    field_name,
                    direction,
                )
            }
        };

        let page_size =
            args.pagination.first.get() * std::cmp::max(1, select_fields_vec.len() as u64);

        let limit = format!(
            r#"
                -- We multiply the value by the number of fields we selected
                -- and add + 1 for the "has next page" flag
                LIMIT
                {page_size} + 1
            "#
        );

        let query_rows = query_as::<_, QueryRow>(&format!(
            "
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
                operation_fields_v1.name,
                operation_fields_v1.value,
                documents.document_view_id,
                documents.document_id

            FROM
                documents
                LEFT JOIN
                    document_view_fields
                    ON
                        document_view_fields.document_view_id = documents.document_view_id
                LEFT JOIN
                    operation_fields_v1
                    ON
                        operation_fields_v1.operation_id = document_view_fields.operation_id
                    AND
                        operation_fields_v1.name = document_view_fields.name

            WHERE
                -- Schema id of the collection we're looking at
                documents.schema_id = '{schema_id}'

                -- application fields get selected here
                {and_application_fields}

                -- cursor pagination
                {and_pagination}

                -- .. now all the filters come ...
                {and_filters}

            {group}

            {order}

            {limit}
            ",
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;

        Ok(vec![])
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
