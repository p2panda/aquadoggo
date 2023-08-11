-- SPDX-License-Identifier: AGPL-3.0-or-later

ALTER TABLE document_views ADD COLUMN document_id TEXT NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE;