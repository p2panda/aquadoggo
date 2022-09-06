-- SPDX-License-Identifier: AGPL-3.0-or-later

CREATE TABLE IF NOT EXISTS logs (
    public_key        TEXT      NOT NULL,
    document          TEXT      NOT NULL,
    -- Store u64 integer as text
    log_id            TEXT      NOT NULL,
    schema            TEXT      NOT NULL,
    PRIMARY KEY (public_key, document, log_id),
    UNIQUE(public_key, log_id)
);

-- Create an index for querying by schema
CREATE INDEX idx_logs_schema ON logs (public_key, log_id, schema);

-- Create an index for sorting by log id
CREATE INDEX idx_logs_by_id ON logs (CAST(log_id AS NUMERIC));
