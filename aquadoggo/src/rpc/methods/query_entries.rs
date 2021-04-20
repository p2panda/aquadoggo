use crate::db::models::Entry;
use crate::errors::Result;
use crate::{
    db::Pool,
    rpc::{request::QueryEntriesRequest, response::QueryEntriesResponse},
};

pub async fn query_entries(
    pool: Pool,
    params: QueryEntriesRequest,
) -> Result<QueryEntriesResponse> {
    let entries = Entry::by_schema(&pool, &params.schema).await?;
    Ok(QueryEntriesResponse { entries })
}

#[cfg(test)]
mod tests {
    use crate::rpc::ApiService;
    use crate::test_helpers::{initialize_db, rpc_request, rpc_response};
    use p2panda_rs::atomic::Hash;

    #[async_std::test]
    async fn query_entries() {
        // Prepare test database
        let pool = initialize_db().await;
        let io = ApiService::io_handler(pool);

        let schema = Hash::new_from_bytes(vec![1, 2, 3]).unwrap();

        // Prepare request to API
        let request = rpc_request(
            "panda_queryEntries",
            &format!(
                r#"{{
                    "schema": "{}"
                }}"#,
                schema.as_hex(),
            ),
        );

        // Prepare expected response result
        let response = rpc_response(&format!(
            r#"{{
                "entries": []
            }}"#,
        ));

        // Send request to API and compare response with expected result
        assert_eq!(io.handle_request_sync(&request), Some(response));
    }
}
