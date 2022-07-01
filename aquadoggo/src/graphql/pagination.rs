// SPDX-License-Identifier: AGPL-3.0-or-later

use serde::{Deserialize, Serialize};

/// Generic pagination response of GraphQL connection API.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Paginated<T, C> {
    pub edges: Vec<PaginatedEdges<T, C>>,
    pub page_info: PaginatedInfo,
}

#[derive(Serialize, Deserialize)]
pub struct PaginatedEdges<T, C> {
    pub cursor: C,
    pub node: T,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaginatedInfo {
    pub has_next_page: bool,
}
