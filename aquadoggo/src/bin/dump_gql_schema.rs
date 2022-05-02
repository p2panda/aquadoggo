use aquadoggo::db::provider::SqlStorage;
use aquadoggo::graphql::{PingRoot, QueryRoot, ReplicationRoot, ClientRoot};
use async_graphql::{EmptyMutation, EmptySubscription, Schema};

fn main() {
    let ping_root: PingRoot = Default::default();
    let replication_root = ReplicationRoot::<SqlStorage>::new();
    let client_root = Default::default();
    let query_root = QueryRoot(ping_root, replication_root, client_root);
    let schema = Schema::build(query_root, EmptyMutation, EmptySubscription)
        //.data(context.replication_context)
        // Add more contexts here if you need, eg:
        //.data(context.ping_context)
        .finish();

    let sdl = schema.sdl();

    println!("{sdl}");
}
