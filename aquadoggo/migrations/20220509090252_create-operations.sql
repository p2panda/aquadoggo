-- SPDX-License-Identifier: AGPL-3.0-or-later

CREATE TABLE IF NOT EXISTS operations_v1 (
    author                  TEXT            NOT NULL,
    document_id             TEXT            NOT NULL,
    operation_id            TEXT            NOT NULL UNIQUE,
    action                  TEXT            NOT NULL,
    schema_id               TEXT            NOT NULL,
    previous_operations     TEXT            NULL,
    PRIMARY KEY (operation_id)
);

CREATE TABLE IF NOT EXISTS operation_fields_v1 (
    operation_id            TEXT             NOT NULL,
    name                    TEXT             NOT NULL,
    field_type              TEXT             NOT NULL,
    value                   TEXT             NULL,
    list_index              NUMERIC          NOT NULL,
    FOREIGN KEY(operation_id) REFERENCES operations_v1(operation_id)
);

CREATE INDEX idx_operation_fields_v1 ON operation_fields_v1 (operation_id, name);
