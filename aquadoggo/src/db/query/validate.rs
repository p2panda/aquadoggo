// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::schema::Schema;

use crate::db::errors::QueryError;
use crate::db::query::{Filter, Order};

pub fn validate_query(filter: &Filter, order: &Order, schema: &Schema) -> Result<(), QueryError> {
    // @TODO
    Ok(())
}
