-- SPDX-License-Identifier: AGPL-3.0-or-later

CREATE TABLE IF NOT EXISTS operations_v1 (
    author              VARCHAR(64)       NOT NULL,
    document_id         VARCHAR(64)       NOT NULL,
    operation_id        VARCHAR(64)       NOT NULL UNIQUE,
    entry_hash          VARCHAR(68)       NOT NULL UNIQUE,
    action              VARCHAR(16)       NOT NULL,
    schema_id           TEXT              NOT NULL,
    previous_operations TEXT              NULL,
    -- FOREIGN KEY(entry_hash) REFERENCES entries(entry_hash),
    PRIMARY KEY (operation_id)
);

-- With the above "previous_operations" column in operations_v1 table we technically no longer need this 
-- relation table. Can we remove it or are there reasons it's useful?
CREATE TABLE IF NOT EXISTS previous_operations_v1 (
    parent_operation_id    VARCHAR(64)       NOT NULL,
    child_operation_id     VARCHAR(64)       NOT NULL,
    FOREIGN KEY(parent_operation_id) REFERENCES operations_v1(operation_id)
    FOREIGN KEY(child_operation_id)  REFERENCES operations_v1(operation_id)
);

CREATE TABLE IF NOT EXISTS operation_fields_v1 (
    operation_id           VARCHAR(64)       NOT NULL,
    name                   VARCHAR(128)      NOT NULL,
    field_type             TEXT              NOT NULL,
    value                  BLOB              NOT NULL,
    FOREIGN KEY(operation_id) REFERENCES operations_v1(operation_id)
);

CREATE INDEX idx_operation_fields_v1 ON operation_fields_v1 (operation_id, name);
