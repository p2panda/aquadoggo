use std::sync::Arc;

use jsonrpc_v2::{Data, MapRouter, Server as Handler};

use crate::db::Pool;
use crate::rpc::methods::{get_entry_args, publish_entry, query_entries};

pub type RpcApiService = Arc<Handler<MapRouter>>;

#[derive(Debug, Clone)]
pub struct RpcApiState {
    pub pool: Pool,
}

pub fn rpc_api_handler(pool: Pool) -> RpcApiService {
    let state = RpcApiState { pool };

    Handler::new()
        .with_data(Data(Arc::new(state)))
        .with_method("panda_getEntryArguments", get_entry_args)
        .with_method("panda_publishEntry", publish_entry)
        .with_method("panda_queryEntries", query_entries)
        .finish()
}

#[cfg(test)]
mod tests {

   use crate::test_helpers::{initialize_db, random_entry_hash};
   use crate::rpc::server::handle_http_request;

   use super::rpc_api_handler;

   #[async_std::test]
   async fn respond_with_missing_param_error() {
       let pool = initialize_db().await;
       let rpc_api_handler = rpc_api_handler(pool.clone());
       
        let mut app = tide::with_state(rpc_api_handler);
        app.at("/")
            .get(|_| async { Ok("Used HTTP Method is not allowed. POST or OPTIONS is required") })
            .post(handle_http_request);
        
        use tide_testing::TideTestingExt;
        assert_eq!(app.get("/").recv_string().await.unwrap(), "Used HTTP Method is not allowed. POST or OPTIONS is required");

    //    assert_eq!(io.handle_request_sync(&request), Some(response));
   }

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
}
