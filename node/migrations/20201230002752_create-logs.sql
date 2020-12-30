CREATE TABLE IF NOT EXISTS logs (
    author            VARCHAR(32)       NOT NULL,
    log_id            BIGINT            NOT NULL,
    schema            VARCHAR(32)       NOT NULL,
    PRIMARY KEY (author, schema)
);
