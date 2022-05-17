-- SPDX-License-Identifier: AGPL-3.0-or-later

CREATE TABLE IF NOT EXISTS entries (
    author            TEXT      NOT NULL,
    entry_bytes       TEXT      NOT NULL,
    entry_hash        TEXT      NOT NULL UNIQUE,
    -- Store u64 integer as character string
    log_id            TEXT      NOT NULL,
    payload_bytes     TEXT,
    payload_hash      TEXT      NOT NULL,
    -- Store u64 integer as character string
    seq_num           TEXT      NOT NULL,
    PRIMARY KEY (author, log_id, seq_num)
);

-- Create an index for sorting by sequence number
CREATE INDEX idx_entries_by_seq_num ON entries (CAST(seq_num AS NUMERIC));
