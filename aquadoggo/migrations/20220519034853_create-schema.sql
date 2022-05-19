-- SPDX-License-Identifier: AGPL-3.0-or-later

CREATE TABLE IF NOT EXISTS schema (
    schema_id         TEXT      NOT NULL,
    name              TEXT      NOT NULL,
    description       TEXT      NOT NULL,
    PRIMARY KEY (schema_id)
);

CREATE TABLE IF NOT EXISTS schema_fields (
    schema_id              TEXT              NOT NULL,
    field_key              TEXT              NOT NULL,
    field_type             TEXT              NOT NULL,
    FOREIGN KEY(schema_id) REFERENCES schema(schema_id)
);

CREATE INDEX idx_schema_fields ON schema_fields (schema_id, field_key);
