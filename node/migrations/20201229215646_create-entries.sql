CREATE TABLE IF NOT EXISTS entries (
    author            VARCHAR(64)       NOT NULL,
    entry_bytes       TEXT              NOT NULL,
    entry_hash        VARCHAR(128)      NOT NULL UNIQUE,
    log_id            UNSIGNED BIGINT   NOT NULL,
    payload_bytes     TEXT,
    payload_hash      VARCHAR(128)      NOT NULL,
    seq_num           UNSIGNED BIGINT   NOT NULL,
    PRIMARY KEY (author, log_id, seq_num)
);
