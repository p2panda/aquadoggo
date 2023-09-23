// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::BTreeMap;
use std::str::FromStr;

use anyhow::Result;
use async_trait::async_trait;
use p2panda_rs::hash::{Hash, HashId};
use p2panda_rs::operation::OperationId;
use p2panda_rs::storage_provider::traits::EntryStore;
use serde::{Deserialize, Serialize};
use unionize::easy::uniform::split;
use unionize::monoid::hashxor::CountingSha256Xor;
use unionize::protocol::{first_message, respond_to_message, SerializableItem};
use unionize::tree::mem_rc;
use unionize::Item as UnionizeItem;

use crate::db::SqlStore;
use crate::replication::errors::ReplicationError;
use crate::replication::strategies::included_document_ids;
use crate::replication::traits::Strategy;
use crate::replication::{Message, Mode, SchemaIdSet, StrategyResult};
use crate::schema::SchemaProvider;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OperationIdItem(OperationId);

impl UnionizeItem for OperationIdItem {
    // Return the "zero" item.
    fn zero() -> Self {
        let hash =
            Hash::new("0020000000000000000000000000000000000000000000000000000000000000").unwrap();
        OperationIdItem(OperationId::new(&hash))
    }

    /// Return the "next" item.
    fn next(&self) -> Self {
        // @TODO: This is a fruity little thing, and I may have misunderstandings as "next" in this
        // context (Peano??) is a little new to me....
        //
        // We want to increment the hexadecimal string of the operation id. To do this we first
        // collect all chars and iterate over their indexes in reverse order (cos we want to
        // increment chars from the end first).
        let mut chars: Vec<char> = self.0.as_str().chars().collect();
        for i in (0..chars.len()).rev() {
            // We don't want to mess with the yasmf prefix, so we break as soon as we get there.
            if i == 3 {
                break;
            };

            // Convert the char we are at into a digit.
            let char = chars.get(i).unwrap();
            let mut digit = char.to_digit(16).unwrap();

            // Increment the digit by 1.
            digit += 1;

            // If the digit is still 15 or less (it is a valid hex char) then update it in the
            // chars vec and break.
            if digit <= 15 {
                chars[i] = char::from_digit(digit, 16).unwrap();
                break;
            }

            // Otherwise we continue in the loop and attempt to increment the next char.
        }
        let hex_str = chars.iter().cloned().collect::<String>();
        OperationIdItem(OperationId::from_str(&hex_str).expect("Valid operation id"))
    }
}

impl SerializableItem for OperationIdItem {}

/// Member of the sets we are unionizing.
pub type Item = OperationIdItem;

/// Monoid hashing function.
pub type Monoid = CountingSha256Xor<Item>;

/// Node which contains the fingerprint tree.
pub type Node = mem_rc::Node<Monoid>;

#[derive(Clone, Debug)]
pub struct SetReconciliationStrategy {
    /// Schema provider for fetching schema by their id.
    schema_provider: SchemaProvider,

    /// Set of schema this session is concerned with.
    target_set: SchemaIdSet,

    /// Object store containing all operation ids in this sessions' set.
    ///
    /// It is required for calls to `respond_to_message`.
    object_store: BTreeMap<OperationIdItem, (OperationIdItem, bool)>,

    /// The same operations as above but in their topo sorted order per document.
    ///
    /// We are strict about the order in which operations should be sent to another node (they
    /// must be able to be directly added to a document) and the operation ids in this set are
    /// sorted correctly to fulfil this condition. We make use of this when processing `wants`
    /// received during set reconciliation.
    ///
    /// @TODO: This is really a funky little workaround, we could combine it with the "object_store"
    /// or come up with another way to manage this without the slightly confusing duplication.
    topo_sorted_operations: Vec<OperationIdItem>,

    /// Ids of all operations we should send the payloads (entry+operation) for.
    ///
    /// We keep track of these as responses come in and only send them all at the end of a
    /// session. This is again to deal with the strict validation the remote node will perform. We
    /// can't send operations out of order.
    ///
    /// @TODO: Shame we can't send payloads incrementally as responses come. Need to consider if
    /// there's a way to handle this better.
    send_operation_ids: Vec<OperationIdItem>,

    /// The initiated state of this session.
    is_initiated: bool,
}

impl SetReconciliationStrategy {
    pub fn new(target_set: &SchemaIdSet, schema_provider: SchemaProvider) -> Self {
        Self {
            schema_provider,
            target_set: target_set.clone(),
            topo_sorted_operations: Vec::new(),
            object_store: BTreeMap::new(),
            send_operation_ids: Vec::new(),
            is_initiated: false,
        }
    }
}

#[async_trait]
impl Strategy for SetReconciliationStrategy {
    fn mode(&self) -> Mode {
        Mode::SetReconciliation
    }

    fn target_set(&self) -> SchemaIdSet {
        self.target_set.clone()
    }

    async fn initial_messages(&mut self, store: &SqlStore) -> StrategyResult {
        // Initiate the session, this fetches operation ids and keeps them in memory.
        self.init(&store).await;

        // Build the tree.
        //
        // @TODO: I want to keep this in memory, stored on the strategy struct, but as `Strategy`
        // requires `Sync` and `Send` but `Node` contains an `Rc` it wasn't possible yet. For now
        // we build the tree from the operation ids we hold in memory every time we respond to a message.
        let mut tree = Node::nil();

        // Insert all operation ids into the tree.
        for operation_id in self.object_store.keys() {
            tree = tree.insert(operation_id.clone());
        }

        // Generate the first message.
        let msg = first_message(&tree).unwrap();

        StrategyResult {
            is_local_done: false,
            messages: vec![Message::SetReconciliation(msg)],
        }
    }

    async fn handle_message(
        &mut self,
        store: &SqlStore,
        message: &Message,
    ) -> Result<StrategyResult, ReplicationError> {
        let mut result = StrategyResult {
            is_local_done: false,
            messages: vec![],
        };

        // If we weren't the one's sending the first message then we also need to initiate the session.
        if !self.is_initiated {
            self.init(&store).await;
        };

        let response = match message {
            Message::SetReconciliation(message) => {
                // Construct the tree.
                let mut tree = Node::nil();

                // Insert all operation ids into the tree.
                for operation_id in self.object_store.keys() {
                    tree = tree.insert(operation_id.clone());
                }

                // Generate a response message.
                let (response, _new_objects) =
                    respond_to_message(&tree, &self.object_store, &message, 3, split::<2>).unwrap();

                response
            }
            _ => {
                return Err(ReplicationError::StrategyFailed(
                    "Received unknown message type".into(),
                ));
            }
        };

        // Store the contents of our responses "provide" for use later.
        //
        // This are the operations we should send payloads (entry+operation) for at the end of the session.
        self.send_operation_ids
            .extend(response.provide().iter().map(|(item, _)| item.to_owned()));

        // Check if the session is ended.
        let is_end = response.is_end();

        // Push the unionize message to the result messages.
        result.messages.push(Message::SetReconciliation(response));

        // If the session has finished, we can send all payloads (entry+operation) now as well.
        if is_end {
            // We iterate over all the operation ids in this session in order to send any wanted ones
            // in the order a remote would expect (otherwise it might not pass validation).
            for operation_id in self.topo_sorted_operations.iter() {
                // If this operation is wanted, get the entry it's encoded on and generate an `Entry` message.
                if self.send_operation_ids.contains(operation_id) {
                    let entry = store
                        .get_entry(operation_id.0.as_hash())
                        .await
                        .expect("Fatal database error")
                        .expect("Entry should be in store");

                    // Push the `Entry` response to the result messages.
                    result.messages.push(Message::Entry(
                        entry.clone().encoded_entry,
                        entry.payload().cloned(),
                    ))
                }
            }

            // Flip the `is_local_done` flag as we are done!
            result.is_local_done = true;
        }

        Ok(result)
    }
}

impl SetReconciliationStrategy {
    pub async fn init(&mut self, store: &SqlStore) {
        // Calculate which documents should be included in the log height.
        let included_document_ids =
            included_document_ids(store, &self.schema_provider, &self.target_set()).await;

        // We calculate and keep in memory the operation ids here, they will be used for the
        // remainder of this session.
        let operation_ids = store
            .get_operation_ids_by_document_id_batch(&included_document_ids)
            .await
            .expect("Fatal database error");

        for operation_id in operation_ids {
            let item = OperationIdItem(operation_id);
            self.topo_sorted_operations.push(item.clone());
            self.object_store.insert(item.clone(), (item, true));
        }

        self.is_initiated = true
    }
}
#[cfg(test)]
mod tests {
    use p2panda_rs::storage_provider::traits::OperationStore;
    use p2panda_rs::test_utils::memory_store::helpers::PopulateStoreConfig;
    use p2panda_rs::Human;
    use rstest::rstest;
    use tokio::sync::broadcast;

    use crate::replication::manager::SyncManager;
    use crate::replication::{Mode, SchemaIdSet, SyncIngest};
    use crate::test_utils::{
        doggo_schema, populate_and_materialize, populate_store_config, test_runner_with_manager,
        TestNodeManager,
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

    #[rstest]
    fn sync_lifetime(
        // This config generates 200 operations on the node by two authors, one author is the
        // default test author (the operations will be the same on each node) and one if a random
        // different author (the operations will not match).
        #[from(populate_store_config)]
        #[with(10, 10, 2)]
        config_200: PopulateStoreConfig,
    ) {
        let peer_id_a: Peer = Peer::new("a");
        let peer_id_b: Peer = Peer::new("b");

        test_runner_with_manager(|manager: TestNodeManager| async move {
            let mut node_a = manager.create().await;
            let mut node_b = manager.create().await;

            // Populate both nodes with 100 matching and 100 unique operations.
            populate_and_materialize(&mut node_a, &config_200).await;
            populate_and_materialize(&mut node_b, &config_200).await;

            // Check the number of operations on each node.
            let node_a_operations = node_a
                .context
                .store
                .get_operations_by_schema_id(doggo_schema().id())
                .await
                .unwrap();

            println!("Node A has {} operations", node_a_operations.len());
            assert_eq!(node_a_operations.len(), 200);

            let node_b_operations = node_b
                .context
                .store
                .get_operations_by_schema_id(doggo_schema().id())
                .await
                .unwrap();

            println!("Node B has {} operations", node_b_operations.len());
            assert_eq!(node_b_operations.len(), 200);

            // Setup sync managers for both peers.
            let (tx, _rx) = broadcast::channel(8);
            let target_set = SchemaIdSet::new(&[config_200.schema.id().to_owned()]);

            let mut manager_a = SyncManager::new(
                node_a.context.store.clone(),
                SyncIngest::new(node_a.context.schema_provider.clone(), tx.clone()),
                peer_id_a.clone(),
            );

            let mut manager_b = SyncManager::new(
                node_b.context.store.clone(),
                SyncIngest::new(node_b.context.schema_provider.clone(), tx),
                peer_id_b.clone(),
            );

            // Generate a `SyncRequest` from peer a.
            let messages = manager_a
                .initiate_session(&peer_id_b, &target_set, &Mode::SetReconciliation)
                .await
                .unwrap();

            println!("Node A: initiate sync session with Node B");

            // Process the `SyncRequest` on peer b.
            let mut result = manager_b
                .handle_message(&peer_id_a, &messages[0])
                .await
                .unwrap();

            println!("Node B: accept sync session with Node A");

            // The next set of sync messages peer b will send.
            let mut peer_b_sync_messages = result.messages;

            println!(
                "Peer B: send {} sync message(s)",
                peer_b_sync_messages.len()
            );

            let mut peer_a_done = false;
            let mut peer_b_done = false;

            loop {
                let mut peer_a_sync_messages = vec![];
                for sync_message in &peer_b_sync_messages {
                    let result = manager_a
                        .handle_message(&peer_id_b, sync_message)
                        .await
                        .unwrap();

                    peer_a_sync_messages.extend(result.messages);
                    peer_a_done = result.is_done;
                }

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
                    result = manager_b
                        .handle_message(&peer_id_a, sync_message)
                        .await
                        .unwrap();

                    peer_b_sync_messages.extend(result.messages);
                    peer_b_done = result.is_done;
                }

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

            println!("Node A has {} operations", node_a_operations.len());
            println!("Node B has {} operations", node_b_operations.len());
            assert_eq!(node_a_operations.len(), node_b_operations.len())
        })
    }
}
