// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::{Context, Object, Result};

#[derive(Default, Debug, Copy, Clone)]
pub struct ReplicationRoot;

#[Object]
impl ReplicationRoot {
    async fn dummy(&self, ctx: &Context<'_>) -> Result<u32> {
        Ok(42)
    }
}
