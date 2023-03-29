// SPDX-License-Identifier: AGPL-3.0-or-later

//! This module offers a query API to find one or many p2panda documents, filtered or sorted by
//! custom parameters. Multiple results are batched via cursor-based pagination.
use p2panda_rs::document::DocumentViewFields;
use p2panda_rs::storage_provider::error::DocumentStorageError;
use sqlx::{query_as, FromRow};

use crate::db::query::{Cursor, Filter, Order, Pagination, Select};
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

impl SqlStore {
    pub async fn query<C: Cursor>(
        &self,
        args: &Query<C>,
    ) -> Result<Vec<DocumentViewFields>, DocumentStorageError> {
        let select_meta = "";
        let select_fields = "";

        let query_rows = query_as::<_, QueryRow>(format!(
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
                -- We always select the `name` of the field
                -- and the `value` of course.
                operation_fields_v1.name,
                operation_fields_v1.value,

                -- Users can query additional meta fields, we
                -- can add them here:
                {select_meta}

                -- We always want an identifier connected
                -- to this data, the document view id is sufficient.
                documents.document_view_id

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
                documents.schema_id = '$1'

                -- `deleted` flag
                AND
                    documents.is_deleted = $2

                -- application fields get selected here
                AND
                    operation_fields_v1.name IN ({select_fields})

                -- .. now all the filters come ...

                -- 1: `profile_eq`
                AND
                    EXISTS (
                        SELECT
                            value
                        FROM
                            operation_fields_v1
                        WHERE
                            operation_fields_v1.name = 'profile'
                            AND
                                operation_fields_v1.operation_id = document_view_fields.operation_id
                            AND
                                operation_fields_v1.value = '00208aca59b674a775852c9d4fdc8c3de6c711e74848c5402e81762cc4c4a981a780'
                    )

                -- 2: `timestamp_gt`
                AND
                    EXISTS (
                        SELECT
                            value
                        FROM
                            operation_fields_v1
                        WHERE
                            operation_fields_v1.name = 'timestamp'
                            AND
                                operation_fields_v1.operation_id = document_view_fields.operation_id
                            AND
                                -- optional type-casting depending on field type
                                CAST(operation_fields_v1.value AS INT) > 1680005105
                    )

            ORDER BY
                -- order by 'timestamp' field, ascending
                (
                    SELECT
                        -- optional type-casting depending on field type
                        CAST(value AS INT)
                    FROM
                        operation_fields_v1
                    WHERE
                        operation_fields_v1.operation_id = document_view_fields.operation_id
                        AND
                            -- name of field to be ordered
                            operation_fields_v1.name = 'timestamp'
                )
                -- choose ASC or DESC
                ASC;
            ",
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DocumentStorageError::FatalStorageError(e.to_string()))?;
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
