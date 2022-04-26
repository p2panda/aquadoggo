-- SPDX-License-Identifier: AGPL-3.0-or-later

CREATE TABLE IF NOT EXISTS operations_v1 (
    author            VARCHAR(64)       NOT NULL,
    operation_id      VARCHAR(64)       NOT NULL UNIQUE,
    entry_hash        VARCHAR(68)       NOT NULL UNIQUE,
    action            VARCHAR(16)       NOT NULL,
    schema_id_short   VARCHAR(68)       NOT NULL,
    PRIMARY KEY (operation_id)
);

CREATE TABLE IF NOT EXISTS previous_operations_v1 (
    parent_operation_id    VARCHAR(64)       NOT NULL,
    child_operation_id     VARCHAR(64)       NOT NULL
);

CREATE TABLE IF NOT EXISTS operation_fields_v1 (
    operation_id                    VARCHAR(64)       NOT NULL,
    name                            VARCHAR(128)      NOT NULL,
    field_type                      TEXT              NOT NULL,
    value                           BLOB              NULL,
    relation_document_id            VARCHAR(64)       NULL,
    relation_document_view_id_hash  VARCHAR(64)       NULL,
    PRIMARY KEY (operation_id, name)
);