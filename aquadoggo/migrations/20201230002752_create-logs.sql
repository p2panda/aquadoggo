-- SPDX-License-Identifier: AGPL-3.0-or-later

CREATE TABLE IF NOT EXISTS logs (
    author            VARCHAR(64)       NOT NULL,
    log_id            BIGINT            NOT NULL,
    schema            VARCHAR(132)      NOT NULL,
    PRIMARY KEY (author, schema, log_id)
);
