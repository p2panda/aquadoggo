-- SPDX-License-Identifier: AGPL-3.0-or-later

CREATE TABLE IF NOT EXISTS tasks (
    name              TEXT      NOT NULL,
    document_id       TEXT      NULL,
    document_view_id  TEXT      NULL
);

-- Create a unique index using `COALESCE`. A regular `UNIQUE` clause will
-- consider two rows that have at least one `null` value to always be distinct
-- but we want to check for equality including `null` values.
-- @TODO: This fails using PostgreSQL
-- CREATE UNIQUE INDEX ux_tasks ON tasks (
    --    name,
    --    COALESCE(document_id, 0),
    --    COALESCE(document_view_id, 0)
    --);

-- Create an index because primary keys can not contain `null` columns.
CREATE INDEX idx_tasks ON tasks (name, document_id, document_view_id);
