-- SPDX-License-Identifier: AGPL-3.0-or-later

-- NOTE: On this level we can not assure eventual task duplicates, this is why we do
-- not have any SQL UNIQUE constraints.

CREATE TABLE IF NOT EXISTS tasks (
    name              TEXT      NOT NULL,
    document_id       TEXT      NULL,
    document_view_id  TEXT      NULL
);
