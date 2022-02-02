-- SPDX-License-Identifier: AGPL-3.0-or-later

CREATE TABLE IF NOT EXISTS logs (
    author            VARCHAR(64)       NOT NULL,
    document          VARCHAR(68)       NOT NULL,
    -- Store u64 integer as 20 character string
    log_id            VARCHAR(20)       NOT NULL,
    schema            VARCHAR(68)       NOT NULL,
    PRIMARY KEY (author, document, log_id),
    UNIQUE(author, log_id)
);

-- Create an index for querying by schema
CREATE INDEX idx_logs_schema ON logs (author, log_id, schema);
