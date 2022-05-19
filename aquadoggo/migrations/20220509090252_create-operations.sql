-- SPDX-License-Identifier: AGPL-3.0-or-later

CREATE TABLE IF NOT EXISTS operations_v1 (
    author                  TEXT            NOT NULL,
    document_id             TEXT            NOT NULL,
    operation_id            TEXT            NOT NULL UNIQUE,
    entry_hash              TEXT            NOT NULL UNIQUE,
    action                  TEXT            NOT NULL,
    schema_id               TEXT            NOT NULL,
    previous_operations     TEXT            NULL,
    PRIMARY KEY (operation_id)
);

-- With the above "previous_operations" column in operations_v1 table we may no longer need this 
-- relation table. Can we remove it or are there reasons it's useful?
CREATE TABLE IF NOT EXISTS previous_operations_v1 (
    parent_operation_id     TEXT             NOT NULL,
    child_operation_id      TEXT             NOT NULL
);

CREATE TABLE IF NOT EXISTS operation_fields_v1 (
    operation_id            TEXT             NOT NULL,
    name                    TEXT             NOT NULL,
    field_type              TEXT             NOT NULL,
    value                   BLOB             NULL,
    FOREIGN KEY(operation_id) REFERENCES operations_v1(operation_id)
);

CREATE INDEX idx_operation_fields_v1 ON operation_fields_v1 (operation_id, name);
