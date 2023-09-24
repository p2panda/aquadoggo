use human_bytes::human_bytes;
use p2panda_rs::identity::KeyPair;
use p2panda_rs::serde::serialize_from;
use p2panda_rs::storage_provider::traits::OperationStore;
use p2panda_rs::Human;
use rstest::rstest;
use serial_test::serial;
use tokio::sync::broadcast;

use crate::replication::manager::SyncManager;
use crate::replication::{Mode, SchemaIdSet, SyncIngest, SyncMessage, ENTRY_TYPE};
use crate::test_utils::{
    doggo_schema, populate_and_materialize, populate_store_config, test_runner_with_manager,
    PopulateStoreConfig, TestNode, TestNodeManager,
};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
struct Peer(String);

impl Peer {
    pub fn new(id: &str) -> Self {
        Self(id.to_string())
    }
}

impl Human for Peer {
    fn display(&self) -> String {
        self.0.clone()
    }
}

fn generate_key_pairs(num: u64) -> Vec<KeyPair> {
    (0..num).map(|_| KeyPair::new()).collect()
}

pub async fn run_protocol(
    node_a: &mut TestNode,
    node_b: &mut TestNode,
    target_set: &SchemaIdSet,
    mode: &Mode,
) {
    println!("");
    println!("RUN PROTOCOL");
    println!("");

    let peer_id_a: Peer = Peer::new("a");
    let peer_id_b: Peer = Peer::new("b");

    // Setup sync managers for both peers.
    let (tx, _rx) = broadcast::channel(8);

    let mut sync_manager_a = SyncManager::new(
        node_a.context.store.clone(),
        SyncIngest::new(node_a.context.schema_provider.clone(), tx.clone()),
        peer_id_a.clone(),
    );

    let mut sync_manager_b = SyncManager::new(
        node_b.context.store.clone(),
        SyncIngest::new(node_b.context.schema_provider.clone(), tx),
        peer_id_b.clone(),
    );

    // Generate a `SetReconciliation` `SyncRequest` from peer a.
    let peer_a_sync_messages = sync_manager_a
        .initiate_session(&peer_id_b, &target_set, &mode)
        .await
        .unwrap();

    println!("Node A: initiate sync session with Node B");

    // Process the `SyncRequest` on peer b.
    let mut result = sync_manager_b
        .handle_message(&peer_id_a, &peer_a_sync_messages[0])
        .await
        .unwrap();

    println!("Node B: accept sync session with Node A");

    // The next set of sync messages peer b will send.
    let mut peer_b_sync_messages = result.messages;

    println!(
        "Peer B: send {} sync message(s)",
        peer_b_sync_messages.len()
    );

    let mut peer_a_all_sent_messages = peer_a_sync_messages.clone();
    let mut peer_b_all_sent_messages = peer_b_sync_messages.clone();
    let mut peer_a_done = false;
    let mut peer_b_done = false;

    loop {
        let mut peer_a_sync_messages = vec![];
        for sync_message in &peer_b_sync_messages {
            let result = sync_manager_a
                .handle_message(&peer_id_b, sync_message)
                .await
                .unwrap();

            peer_a_sync_messages.extend(result.messages);
            peer_a_done = result.is_done;
        }
        peer_a_all_sent_messages.extend(peer_a_sync_messages.clone());

        println!(
            "Peer A: send {} sync message(s)",
            peer_a_sync_messages.len()
        );

        if peer_a_done {
            println!("Peer A: is done!");
        }

        if peer_a_done && peer_b_done {
            break;
        }

        peer_b_sync_messages = vec![];
        for sync_message in &peer_a_sync_messages {
            result = sync_manager_b
                .handle_message(&peer_id_a, sync_message)
                .await
                .unwrap();

            peer_b_sync_messages.extend(result.messages);
            peer_b_done = result.is_done;
        }
        peer_b_all_sent_messages.extend(peer_b_sync_messages.clone());

        println!(
            "Peer B: send {} sync message(s)",
            peer_b_sync_messages.len()
        );

        if peer_b_done {
            println!("Peer B: is done!");
        }

        if peer_a_done && peer_b_done {
            break;
        }
    }

    let protocol_messages_to_bytes = |message: &SyncMessage| {
        if message.message_type() != ENTRY_TYPE {
            Some(serialize_from(message))
        } else {
            None
        }
    };

    let payload_messages_to_bytes = |message: &SyncMessage| {
        if message.message_type() == ENTRY_TYPE {
            Some(serialize_from(message))
        } else {
            None
        }
    };

    let total_protocol_bytes_sent_peer_a = peer_a_all_sent_messages
        .iter()
        .filter_map(protocol_messages_to_bytes)
        .fold(0.0, |acc, bytes| acc + bytes.len() as f64);

    let total_protocol_bytes_sent_peer_b = peer_b_all_sent_messages
        .iter()
        .filter_map(protocol_messages_to_bytes)
        .fold(0.0, |acc, bytes| acc + bytes.len() as f64);

    let total_payload_bytes_sent_peer_a = peer_a_all_sent_messages
        .iter()
        .filter_map(payload_messages_to_bytes)
        .fold(0.0, |acc, bytes| acc + bytes.len() as f64);

    let total_payload_bytes_sent_peer_b = peer_b_all_sent_messages
        .iter()
        .filter_map(payload_messages_to_bytes)
        .fold(0.0, |acc, bytes| acc + bytes.len() as f64);

    let node_a_operations = node_a
        .context
        .store
        .get_operations_by_schema_id(doggo_schema().id())
        .await
        .unwrap();

    let node_b_operations = node_b
        .context
        .store
        .get_operations_by_schema_id(doggo_schema().id())
        .await
        .unwrap();

    println!("");
    println!("RESULTS!");
    println!("");
    println!("Node A has {} operations", node_a_operations.len());
    println!("Node B has {} operations", node_b_operations.len());
    assert_eq!(node_a_operations.len(), node_b_operations.len());
    println!(
        "Node A sent {} of protocol and {} of payload messages",
        human_bytes(total_protocol_bytes_sent_peer_a),
        human_bytes(total_payload_bytes_sent_peer_a),
    );
    println!(
        "Node B sent {} of protocol and {} of payload messages",
        human_bytes(total_protocol_bytes_sent_peer_b),
        human_bytes(total_payload_bytes_sent_peer_b),
    );
    println!("");
}

#[rstest]
#[serial]
fn sync_lifetime(
    #[from(populate_store_config)]
    #[with(2, 10)]
    mut config: PopulateStoreConfig,
    #[values(100)] authors_on_node: u64,
    #[values(0.0, 0.5, 1.0)] shared_state_ratio: f64,
    #[values(Mode::SetReconciliation, Mode::LogHeight)] mode: Mode,
) {
    test_runner_with_manager(move |manager: TestNodeManager| async move {
        let mut node_a = manager.create().await;
        let mut node_b = manager.create().await;
        let num_common_key_pairs = (authors_on_node as f64 * shared_state_ratio) as u64;

        println!("START SYNC TEST");
        println!("Mode: {}", mode.as_str());
        println!("");

        println!("GENERATING TEST DATA");
        println!("Common authors: {}", num_common_key_pairs);
        println!(
            "Distinct authors: {}",
            authors_on_node - num_common_key_pairs
        );
        println!("Documents per author: {}", config.no_of_logs);
        println!("Operations per document: {}", config.no_of_entries);
        println!("");
        println!(
            "Generating common test data on nodes A and B for {} authors",
            num_common_key_pairs
        );

        let common_key_pairs = generate_key_pairs(num_common_key_pairs);
        config.authors = common_key_pairs;
        populate_and_materialize(&mut node_a, &config).await;
        populate_and_materialize(&mut node_b, &config).await;

        println!(
            "Generating unique test data on node A for {} authors",
            authors_on_node - num_common_key_pairs
        );

        config.authors = generate_key_pairs(authors_on_node - num_common_key_pairs);
        populate_and_materialize(&mut node_a, &config).await;

        println!(
            "Generating unique test data on node B for {} authors",
            authors_on_node - num_common_key_pairs
        );

        config.authors = generate_key_pairs(authors_on_node - num_common_key_pairs);
        populate_and_materialize(&mut node_b, &config).await;

        let node_a_operations = node_a
            .context
            .store
            .get_operations_by_schema_id(doggo_schema().id())
            .await
            .unwrap();

        println!("Node A has {} operations", node_a_operations.len());

        let node_b_operations = node_b
            .context
            .store
            .get_operations_by_schema_id(doggo_schema().id())
            .await
            .unwrap();

        println!("Node B has {} operations", node_b_operations.len());

        let target_set = SchemaIdSet::new(&[config.schema.id().to_owned()]);

        run_protocol(&mut node_a, &mut node_b, &target_set, &mode).await;

        let node_a_operations = node_a
            .context
            .store
            .get_operations_by_schema_id(doggo_schema().id())
            .await
            .unwrap();

        let node_b_operations = node_b
            .context
            .store
            .get_operations_by_schema_id(doggo_schema().id())
            .await
            .unwrap();

        assert_eq!(node_a_operations, node_b_operations)
    })
}
