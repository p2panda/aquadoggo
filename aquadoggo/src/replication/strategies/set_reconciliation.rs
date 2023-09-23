// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use async_trait::async_trait;
use p2panda_rs::hash::HashId;
use p2panda_rs::operation::OperationId;
use unionize::item::le_byte_array::LEByteArray;
use unionize::monoid::hashxor::CountingSha256Xor;
use unionize::tree::mem_rc;

use crate::db::SqlStore;
use crate::replication::errors::ReplicationError;
use crate::replication::strategies::included_document_ids;
use crate::replication::traits::Strategy;
use crate::replication::{Message, Mode, SchemaIdSet, StrategyResult};
use crate::schema::SchemaProvider;

/// Convert OperationId into LEByteArray<34> which is a supported unionize item.
fn to_item(operation: &OperationId) -> LEByteArray<34> {
    let mut buf = [0u8; 34];
    let operation_bs = operation.to_bytes();
    for i in 0..34.min(operation_bs.len()) {
        buf[33 - i] = operation_bs[i];
    }

    LEByteArray(buf)
}

/// A member of the sets we are unionizing.
pub type Item = LEByteArray<34>;

/// The monoid hashing function.
pub type Monoid = CountingSha256Xor<Item>;

/// Node which contains the item tree.
pub type Node = mem_rc::Node<Monoid>;

#[derive(Clone, Debug)]
pub struct SetReconciliationStrategy {
    schema_provider: SchemaProvider,
    target_set: SchemaIdSet,
    received_remote_have: bool,
    sent_have: bool,
}

impl SetReconciliationStrategy {
    pub fn new(target_set: &SchemaIdSet, schema_provider: SchemaProvider) -> Self {
        Self {
            schema_provider,
            target_set: target_set.clone(),
            received_remote_have: false,
            sent_have: false,
        }
    }
}

#[async_trait]
impl Strategy for SetReconciliationStrategy {
    fn mode(&self) -> Mode {
        Mode::LogHeight
    }

    fn target_set(&self) -> SchemaIdSet {
        self.target_set.clone()
    }

    async fn initial_messages(&mut self, store: &SqlStore) -> StrategyResult {
        // Calculate which documents should be included in the log height.
        let included_document_ids =
            included_document_ids(store, &self.schema_provider, &self.target_set()).await;

        // @TODO: build tree over included documents' operation ids, persist on session.
        // @TODO: calculate initial messages.

        self.sent_have = true;

        // @TODO: return strategy response.
        todo!()
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

        // Send our Have message to remote if we haven't done it yet
        if !self.sent_have {
            result.merge(self.initial_messages(store).await);
        }

        // @TODO: process messages and calculate response.

        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::iter::FromIterator;

    use p2panda_rs::test_utils::fixtures::random_operation_id;
    use rstest::rstest;
    use unionize::easy::uniform::split;
    use unionize::protocol::{first_message, respond_to_message};

    use super::{to_item, Item, Node};

    #[rstest]
    fn test_run() {
        let mut operation_ids = vec![];
        for _ in 0..10 {
            operation_ids.push(random_operation_id())
        }

        operation_ids.sort();
        let items_party_a: Vec<Item> = operation_ids[..5].iter().map(to_item).collect();
        let items_party_b: Vec<Item> = operation_ids[3..].iter().map(to_item).collect();

        let item_set_a: BTreeSet<Item> = BTreeSet::from_iter(items_party_a.iter().cloned());
        let item_set_b: BTreeSet<Item> = BTreeSet::from_iter(items_party_b.iter().cloned());

        let intersection: Vec<_> = item_set_a.intersection(&item_set_b).cloned().collect();

        println!("a items: {item_set_a:?}");
        println!("b items: {item_set_b:?}");

        println!("intersection: {intersection:?}");

        let mut root_a = Node::nil();
        let mut root_b = Node::nil();

        let mut object_store_a = BTreeMap::new();
        let mut object_store_b = BTreeMap::new();

        for item in item_set_a.iter().cloned() {
            root_a = root_a.insert(item);
            object_store_a.insert(item, (item, true));
        }

        for item in item_set_b.iter().cloned() {
            root_b = root_b.insert(item);
            object_store_b.insert(item, (item, true));
        }

        let mut msg = first_message(&root_a).unwrap();

        let mut missing_items_a = vec![];
        let mut missing_items_b = vec![];

        loop {
            println!("a msg: {msg:?}");
            if msg.is_end() {
                break;
            }

            println!("b-----");
            let (resp, new_objects) =
                respond_to_message(&root_b, &object_store_b, &msg, 3, split::<2>).unwrap();
            missing_items_b.extend(new_objects.into_iter().map(|(item, _)| item));

            println!("b msg: {resp:?}");
            if resp.is_end() {
                break;
            }

            println!("a-----");
            let (resp, new_objects) =
                respond_to_message(&root_a, &object_store_a, &resp, 3, split::<2>).unwrap();
            missing_items_a.extend(new_objects.into_iter().map(|(item, _)| item));

            msg = resp;
        }

        println!("a all: {item_set_a:?} + {missing_items_a:?}");
        println!("b all: {item_set_b:?} + {missing_items_b:?}");

        assert_eq!(missing_items_a.len(), item_set_b.len() - intersection.len());
        assert_eq!(missing_items_b.len(), item_set_a.len() - intersection.len());

        let mut all_items = item_set_a.clone();
        let mut all_items_a = item_set_a.clone();
        let mut all_items_b = item_set_b.clone();
        all_items.extend(item_set_b.iter());
        all_items_a.extend(missing_items_a.iter());
        all_items_b.extend(missing_items_b.iter());

        let mut a_all: Vec<Item> = Vec::from_iter(all_items_a.iter().cloned());
        let mut b_all: Vec<Item> = Vec::from_iter(all_items_b.iter().cloned());
        let mut all: Vec<Item> = Vec::from_iter(all_items.iter().cloned());

        a_all.sort();
        b_all.sort();
        all.sort();

        println!("\n  all vec: {all:?}");
        println!(
            "\na all vec: {a_all:?}, {:} {:}",
            a_all == all,
            all == a_all
        );
        println!(
            "\nb all vec: {b_all:?}, {:} {:}",
            b_all == all,
            all == b_all
        );
        println!();

        let a_eq = a_all == all;
        let b_eq = b_all == all;

        assert!(a_eq, "a does not match");
        assert!(b_eq, "a does not match");

        println!("{a_eq}, {b_eq}");
    }
}
