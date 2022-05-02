// SPDX-License-Identifier: AGPL-3.0-or-later

use crate::db::Pool;

pub struct SqlStorage {
    pub(crate) pool: Pool,
}
