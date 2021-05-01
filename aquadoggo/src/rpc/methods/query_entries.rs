use jsonrpc_v2::{Data, Params};
use p2panda_rs::atomic::Validation;

use crate::db::models::Entry;
use crate::errors::Result;
use crate::rpc::request::QueryEntriesRequest;
use crate::rpc::response::QueryEntriesResponse;
use crate::rpc::RpcApiState;

pub async fn query_entries(
    data: Data<RpcApiState>,
    Params(params): Params<QueryEntriesRequest>,
) -> Result<QueryEntriesResponse> {
    // Validate request parameters
    params.schema.validate()?;

    // Get database connection pool
    let pool = data.pool.clone();

    // Find and return raw entries from database
    let entries = Entry::by_schema(&pool, &params.schema).await?;
    Ok(QueryEntriesResponse { entries })
}

// @TODO
// #[cfg(test)]
// mod tests {
//     use crate::rpc::ApiService;
//     use crate::test_helpers::{initialize_db, rpc_request, rpc_response};
//     use p2panda_rs::atomic::Hash;

//     #[async_std::test]
//     async fn query_entries() {
//         // Prepare test database
//         let pool = initialize_db().await;
//         let io = ApiService::io_handler(pool);

//         let schema = Hash::new_from_bytes(vec![1, 2, 3]).unwrap();

//         // Prepare request to API
//         let request = rpc_request(
//             "panda_queryEntries",
//             &format!(
//                 r#"{{
//                     "schema": "{}"
//                 }}"#,
//                 schema.as_hex(),
//             ),
//         );

//         // Prepare expected response result
//         let response = rpc_response(&format!(
//             r#"{{
//                 "entries": []
//             }}"#,
//         ));

//         // Send request to API and compare response with expected result
//         assert_eq!(io.handle_request_sync(&request), Some(response));
//     }
// }
