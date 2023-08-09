// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;
use std::net::{SocketAddr, TcpListener};

use axum::body::HttpBody;
use axum::BoxError;
use http::header::{HeaderName, HeaderValue};
use http::{Request, StatusCode};
use hyper::{Body, Server};
use tokio::sync::broadcast;
use tower::make::Shared;
use tower_service::Service;

use crate::graphql::GraphQLSchemaManager;
use crate::http::{build_server, HttpServiceContext, BLOBS_ROUTE};
use crate::test_utils::TestNode;

/// GraphQL client which can be used for querying a node in tests.
pub struct TestClient {
    client: reqwest::Client,
    addr: SocketAddr,
}

impl TestClient {
    pub(crate) fn new<S, ResBody>(service: S) -> Self
    where
        S: Service<Request<Body>, Response = http::Response<ResBody>> + Clone + Send + 'static,
        ResBody: HttpBody + Send + 'static,
        ResBody::Data: Send,
        ResBody::Error: Into<BoxError>,
        S::Future: Send,
        S::Error: Into<BoxError>,
    {
        // Setting the port to zero asks the operating system to find one for us
        let listener = TcpListener::bind("127.0.0.1:0").expect("Could not bind ephemeral socket");
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let server = Server::from_tcp(listener)
                .unwrap()
                .serve(Shared::new(service));
            server.await.expect("server error");
        });

        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();

        TestClient { client, addr }
    }

    #[allow(dead_code)]
    pub(crate) fn get(&self, url: &str) -> RequestBuilder {
        RequestBuilder {
            builder: self.client.get(format!("http://{}{}", self.addr, url)),
        }
    }

    pub(crate) fn post(&self, url: &str) -> RequestBuilder {
        RequestBuilder {
            builder: self.client.post(format!("http://{}{}", self.addr, url)),
        }
    }
}

/// Configures a test client that can be used for GraphQL testing.
pub async fn graphql_test_client(node: &TestNode) -> TestClient {
    let (tx, _) = broadcast::channel(120);
    let manager = GraphQLSchemaManager::new(
        node.context.store.clone(),
        tx,
        node.context.schema_provider.clone(),
    )
    .await;
    let http_context = HttpServiceContext::new(manager, BLOBS_ROUTE.into());
    TestClient::new(build_server(http_context))
}

pub(crate) struct RequestBuilder {
    builder: reqwest::RequestBuilder,
}

impl RequestBuilder {
    pub(crate) async fn send(self) -> TestResponse {
        TestResponse {
            response: self.builder.send().await.unwrap(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn body(mut self, body: impl Into<reqwest::Body>) -> Self {
        self.builder = self.builder.body(body);
        self
    }

    pub(crate) fn json<T>(mut self, json: &T) -> Self
    where
        T: serde::Serialize,
    {
        self.builder = self.builder.json(json);
        self
    }

    #[allow(dead_code)]
    pub(crate) fn header<K, V>(mut self, key: K, value: V) -> Self
    where
        HeaderName: TryFrom<K>,
        <HeaderName as TryFrom<K>>::Error: Into<http::Error>,
        HeaderValue: TryFrom<V>,
        <HeaderValue as TryFrom<V>>::Error: Into<http::Error>,
    {
        self.builder = self.builder.header(key, value);
        self
    }
}

pub(crate) struct TestResponse {
    response: reqwest::Response,
}

impl TestResponse {
    pub(crate) async fn text(self) -> String {
        self.response.text().await.unwrap()
    }

    pub(crate) async fn json<T>(self) -> T
    where
        T: serde::de::DeserializeOwned,
    {
        self.response.json().await.unwrap()
    }

    #[allow(dead_code)]
    pub(crate) fn status(&self) -> StatusCode {
        self.response.status()
    }
}
