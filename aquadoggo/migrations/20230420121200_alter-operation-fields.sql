-- SPDX-License-Identifier: AGPL-3.0-or-later

ALTER TABLE operation_fields_v1 ADD COLUMN cursor TEXT NOT NULL;

CREATE UNIQUE INDEX ux_operation_fields_v1 ON operation_fields_v1 (cursor);
