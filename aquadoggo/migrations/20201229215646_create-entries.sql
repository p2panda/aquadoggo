-- SPDX-License-Identifier: AGPL-3.0-or-later

CREATE TABLE IF NOT EXISTS entries (
    author            VARCHAR(64)       NOT NULL,
    entry_bytes       TEXT              NOT NULL,
    entry_hash        VARCHAR(68)       NOT NULL UNIQUE,
    -- Store u64 integer as 20 character string
    log_id            VARCHAR(20)       NOT NULL,
    payload_bytes     TEXT,
    payload_hash      VARCHAR(68)       NOT NULL,
    -- Store u64 integer as 20 character string
    seq_num           VARCHAR(20)       NOT NULL,
    PRIMARY KEY (author, log_id, seq_num)
);
