-- SPDX-License-Identifier: AGPL-3.0-or-later

CREATE TABLE IF NOT EXISTS tasks (
    name              TEXT      NOT NULL,
    document_id       TEXT      NULL,
    document_view_id  TEXT      NULL,
    PRIMARY KEY (name, document_id, document_view_id)
);
