-- SPDX-License-Identifier: AGPL-3.0-or-later
CREATE TABLE IF NOT EXISTS document_view_fields (
    document_view_id_hash   VARCHAR(64)     NOT NULL,
    -- we wanna use this as a foreign key, how do we make it non-unique
    operation_id            VARCHAR(64)     NOT NULL,
    name                    VARCHAR(128)    NOT NULL
);

CREATE INDEX idx_document_view_fields ON document_view_fields (document_view_id_hash, operation_id, name);

CREATE TABLE IF NOT EXISTS document_views (
    document_view_id_hash       VARCHAR(64)     NOT NULL UNIQUE,
    schema_id                   TEXT            NOT NULL,
    PRIMARY KEY (document_view_id_hash)
);

CREATE TABLE IF NOT EXISTS documents (
    document_id                 VARCHAR(64)     NOT NULL UNIQUE,
    document_view_id_hash       VARCHAR(64)     NOT NULL,
    PRIMARY KEY (document_id)
);