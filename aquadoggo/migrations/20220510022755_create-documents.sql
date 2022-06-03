-- SPDX-License-Identifier: AGPL-3.0-or-later

CREATE TABLE IF NOT EXISTS document_view_fields (
    document_view_id            TEXT            NOT NULL,
    operation_id                TEXT            NOT NULL,
    name                        TEXT            NOT NULL,
    FOREIGN KEY(operation_id)   REFERENCES      operations_v1(operation_id)
);

CREATE INDEX idx_document_view_fields ON document_view_fields (document_view_id, operation_id, name);

CREATE TABLE IF NOT EXISTS document_views (
    document_view_id            TEXT            NOT NULL UNIQUE,
    schema_id                   TEXT            NOT NULL,
    PRIMARY KEY (document_view_id)
);

CREATE TABLE IF NOT EXISTS documents (
    document_id                 TEXT            NOT NULL UNIQUE,
    document_view_id            TEXT            NOT NULL,
    schema_id                   TEXT            NOT NULL,
    is_deleted                  BOOLEAN         NOT NULL,
    PRIMARY KEY (document_id)
);
