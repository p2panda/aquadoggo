// SPDX-License-Identifier: AGPL-3.0-or-later

use std::sync::Arc;

use async_graphql::{EmptySubscription, MergedObject, Schema};
use tokio::sync::Mutex;

use crate::bus::ServiceSender;
use crate::db::provider::SqlStorage;
use crate::graphql::client::{ClientMutationRoot, ClientRoot};
use crate::graphql::replication::context::ReplicationContext;
use crate::graphql::replication::ReplicationRoot;

/// All of the graphql query sub modules merged into one top level root.
#[derive(MergedObject, Debug)]
pub struct QueryRoot(pub ReplicationRoot<SqlStorage>, pub ClientRoot);

/// All of the graphql mutation sub modules merged into one top level root.
#[derive(MergedObject, Debug, Copy, Clone, Default)]
pub struct MutationRoot(pub ClientMutationRoot);

/// GraphQL schema for p2panda node.
pub type RootSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

/// Build the root graphql schema that can handle graphql requests.
pub fn build_root_schema(store: SqlStorage, tx: ServiceSender) -> RootSchema {
    let replication_context = Arc::new(Mutex::new(ReplicationContext::new(1000, store.clone())));

    let replication_root = ReplicationRoot::<SqlStorage>::new();
    let client_query_root = ClientRoot::default();
    let query_root = QueryRoot(replication_root, client_query_root);

    let client_mutation_root = ClientMutationRoot::default();
    let mutation_root = MutationRoot(client_mutation_root);

    Schema::build(query_root, mutation_root, EmptySubscription)
        .data(replication_context)
        .data(store)
        .data(tx)
        .finish()
}
