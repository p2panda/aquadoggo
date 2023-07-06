-- SPDX-License-Identifier: AGPL-3.0-or-later

ALTER TABLE operations_v1 ADD COLUMN sorted_index INT;
ALTER TABLE operations_v1 ADD COLUMN document_view_id TEXT;