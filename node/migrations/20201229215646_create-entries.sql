CREATE TABLE IF NOT EXISTS entries (
    author            VARCHAR(64)       NOT NULL,
    entry_bytes       BINARY            NOT NULL,
    entry_hash        VARCHAR(128)      NOT NULL UNIQUE,
    log_id            UNSIGNED BIGINT   NOT NULL,
    payload_bytes     BINARY,
    payload_hash      VARCHAR(128)      NOT NULL,
    seqnum            UNSIGNED BIGINT   NOT NULL,
    PRIMARY KEY (author, log_id, seqnum)
);
