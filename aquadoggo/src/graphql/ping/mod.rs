// SPDX-License-Identifier: AGPL-3.0-or-later

use std::str::FromStr;

use async_graphql::{Object, };

#[derive(Default)]
pub struct PingRoot;

#[Object]
impl PingRoot {
    // @TODO: Remove this example.
    async fn ping(&self) -> String {
        String::from_str("pong").unwrap()
    }
}
