// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use p2panda_rs::entry::{LogId, SeqNum};
use p2panda_rs::identity::PublicKey;

use crate::db::SqlStore;
use crate::replication::errors::ReplicationError;
use crate::replication::traits::Strategy;
use crate::replication::{Message, Mode, TargetSet};

fn diff_log_heights(
    local_log_heights: &Vec<(PublicKey, Vec<(LogId, SeqNum)>)>,
    remote_log_heights: &Vec<(PublicKey, Vec<(LogId, SeqNum)>)>,
) -> Vec<(PublicKey, Vec<(LogId, SeqNum)>)> {
    let mut remote_needs = Vec::new();
    for (remote_author, remote_author_logs) in remote_log_heights {
        if let Some((_, local_author_logs)) = local_log_heights
            .iter()
            .find(|(local_author, _)| local_author == remote_author)
        {
            let remote_needs_logs = local_author_logs
                .to_owned()
                .into_iter()
                .filter(|(local_log_id, local_seq_num)| {
                    let remote_log = remote_author_logs
                        .iter()
                        .find(|(remote_log_id, _)| local_log_id == remote_log_id);

                    match remote_log {
                        Some((_, remote_seq_num)) => local_seq_num > remote_seq_num,
                        None => true,
                    }
                })
                .collect();
            remote_needs.push((remote_author.to_owned(), remote_needs_logs));
        }
    }
    remote_needs
}

// @TODO: Better name?!!
#[derive(Clone, Debug)]
pub struct StrategyResult {
    pub messages: Vec<Message>,
    pub is_local_done: bool,
}

#[derive(Clone, Debug)]
pub struct NaiveStrategy {
    target_set: TargetSet,
}

impl NaiveStrategy {
    pub fn new(target_set: &TargetSet) -> Self {
        Self {
            target_set: target_set.clone(),
        }
    }
}

#[async_trait]
impl Strategy for NaiveStrategy {
    fn mode(&self) -> Mode {
        Mode::Naive
    }

    fn target_set(&self) -> TargetSet {
        self.target_set.clone()
    }

    async fn initial_messages(&self, store: &SqlStore) -> Vec<Message> {
        let mut target_set_log_heights = vec![];

        // For every schema id in the target set retrieve log heights for all contributing authors
        for schema_id in self.target_set().0.iter() {
            let log_heights = store
                .get_log_heights(schema_id)
                .await
                .expect("Fatal database error");
            target_set_log_heights.extend(log_heights);
        }
        vec![Message::Have(target_set_log_heights)]
    }

    async fn handle_message(
        &self,
        _store: &SqlStore,
        message: &Message,
    ) -> Result<StrategyResult, ReplicationError> {
        // TODO: Verify that the TargetSet contained in the message is a sub-set of the passed
        // local TargetSet.
        let _target_set = self.target_set();
        let messages = Vec::new();
        let mut is_local_done = false;

        match message {
            Message::Have(_log_heights) => {
                // Compose Have message and push to messages
                is_local_done = true;
            }
            Message::Entry(_, _) => {
                // self.handle_entry(..)
            }
            _ => panic!("Naive replication strategy received unsupported message type"),
        }

        Ok(StrategyResult {
            is_local_done,
            messages,
        })
    }
}

#[derive(Clone, Debug)]
pub struct SetReconciliationStrategy();

impl SetReconciliationStrategy {
    pub fn new() -> Self {
        Self()
    }
}

#[async_trait]
impl Strategy for SetReconciliationStrategy {
    fn mode(&self) -> Mode {
        Mode::SetReconciliation
    }

    fn target_set(&self) -> TargetSet {
        todo!()
    }

    async fn initial_messages(&self, _store: &SqlStore) -> Vec<Message> {
        todo!()
    }

    async fn handle_message(
        &self,
        _store: &SqlStore,
        _message: &Message,
    ) -> Result<StrategyResult, ReplicationError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use p2panda_rs::entry::{LogId, SeqNum};
    use p2panda_rs::identity::KeyPair;
    use p2panda_rs::test_utils::fixtures::random_key_pair;
    use rstest::rstest;

    use super::diff_log_heights;

    #[rstest]
    fn correctly_diffs_log_heights(
        #[from(random_key_pair)] author_a: KeyPair,
        #[from(random_key_pair)] author_b: KeyPair,
        #[from(random_key_pair)] author_c: KeyPair,
    ) {
        let author_a = author_a.public_key();
        let author_b = author_b.public_key();
        let author_c = author_c.public_key();
        let peer_a_log_heights = vec![(author_a, vec![(LogId::new(0), SeqNum::new(5).unwrap())])];
        let peer_b_log_heights = vec![(author_a, vec![(LogId::new(0), SeqNum::new(8).unwrap())])];

        let peer_b_needs = diff_log_heights(&peer_a_log_heights, &peer_b_log_heights);
        let peer_a_needs = diff_log_heights(&peer_a_log_heights, &peer_b_log_heights);

        assert_eq!(peer_a_needs, peer_b_log_heights);
        assert_eq!(peer_b_needs, vec![]);
    }
}
