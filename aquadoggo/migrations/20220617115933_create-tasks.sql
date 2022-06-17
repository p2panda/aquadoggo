-- SPDX-License-Identifier: AGPL-3.0-or-later

CREATE TABLE IF NOT EXISTS tasks (
    name              TEXT      NOT NULL,
    operation_id      TEXT      NOT NULL,
    PRIMARY KEY (name, operation_id)
);
