-- SPDX-License-Identifier: AGPL-3.0-or-later

-- The change from schema logs to document logs invalidates all entries so
-- all logs and entries are deleted. The logs table is also dropped because its
-- primary key changes.

DROP TABLE logs;
DELETE FROM entries;

CREATE TABLE logs (
    author            VARCHAR(64)       NOT NULL,
    log_id            BIGINT            NOT NULL,
    document          VARCHAR(132)      NOT NULL,
    schema            VARCHAR(132)      NOT NULL,
    PRIMARY KEY (author, document, log_id)
);

-- Create an index for querying by schema
CREATE INDEX idx_logs_schema ON logs (author, log_id, schema);
