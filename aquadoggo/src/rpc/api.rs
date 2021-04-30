use std::sync::Arc;

use jsonrpc_v2::{Data, MapRouter, Server};

use crate::db::Pool;
use crate::rpc::methods::{get_entry_args, publish_entry, query_entries};

pub type RpcApiService = Arc<Server<MapRouter>>;

#[derive(Debug, Clone)]
pub struct RpcServerState {
    pub pool: Pool,
}

pub fn new_rpc_api_service(pool: Pool) -> RpcApiService {
    let state = RpcServerState { pool };

    Server::new()
        .with_data(Data(Arc::new(state)))
        .with_method("panda_getEntryArguments", get_entry_args)
        .with_method("panda_publishEntry", publish_entry)
        .with_method("panda_queryEntries", query_entries)
        .finish()
}

//#[cfg(test)]
//mod tests {
//    use jsonrpc_core::ErrorCode;

//    use crate::test_helpers::{initialize_db, random_entry_hash, rpc_error, rpc_request};

//    use super::ApiService;

//    #[async_std::test]
//    async fn respond_with_missing_param_error() {
//        let pool = initialize_db().await;
//        let io = ApiService::io_handler(pool);

//        let request = rpc_request(
//            "panda_getEntryArguments",
//            &format!(
//                r#"{{
//                    "schema": "{}"
//                }}"#,
//                random_entry_hash()
//            ),
//        );

//        let response = rpc_error(
//            ErrorCode::InvalidParams,
//            "Invalid params: missing field `author`.",
//        );

//        assert_eq!(io.handle_request_sync(&request), Some(response));
//    }

//    #[async_std::test]
//    async fn respond_with_wrong_author_error() {
//        let pool = initialize_db().await;
//        let io = ApiService::io_handler(pool);

//        let request = rpc_request(
//            "panda_getEntryArguments",
//            &format!(
//                r#"{{
//                    "author": "1234",
//                    "schema": "{}"
//                }}"#,
//                random_entry_hash()
//            ),
//        );

//        let response = rpc_error(
//            ErrorCode::InvalidParams,
//            "Invalid params: invalid author key length.",
//        );

//        assert_eq!(io.handle_request_sync(&request), Some(response));
//    }
//}
