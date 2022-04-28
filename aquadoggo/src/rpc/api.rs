// SPDX-License-Identifier: AGPL-3.0-or-later

use std::sync::Arc;

use jsonrpc_v2::{Data, MapRouter, Server as Service};

use crate::db::store::SqlStorage;
use crate::db::Pool;
use crate::rpc::methods::{get_entry_args, publish_entry, query_entries};

pub type RpcApiService = Arc<Service<MapRouter>>;

#[derive(Debug, Clone)]
pub struct RpcApiState {
    pub pool: Pool,
}

pub fn build_rpc_api_service(pool: Pool) -> RpcApiService {
    let state = SqlStorage { pool };

    Service::new()
        .with_data(Data(Arc::new(state)))
        .with_method("panda_getEntryArguments", get_entry_args)
        .with_method("panda_publishEntry", publish_entry)
        .with_method("panda_queryEntries", query_entries)
        .finish()
}
