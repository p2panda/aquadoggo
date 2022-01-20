-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Adding database tables for the findings app while we don't have a way to
-- create tables automatically from published schemas

CREATE TABLE IF NOT EXISTS `bookmarks-0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b` (
    document          VARCHAR(68)       PRIMARY KEY NOT NULL,
    url               TEXT              NOT NULL,
    title             TEXT              NOT NULL,
    created           TEXT              NOT NULL
);
