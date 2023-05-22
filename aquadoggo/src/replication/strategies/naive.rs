// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use p2panda_rs::entry::{LogId, SeqNum};

use crate::db::SqlStore;
use crate::replication::errors::ReplicationError;
use crate::replication::traits::Strategy;
use crate::replication::{LogHeight, Message, Mode, StrategyResult, TargetSet};

fn diff_log_heights(
    local_log_heights: &Vec<LogHeight>,
    remote_log_heights: &Vec<LogHeight>,
) -> Vec<LogHeight> {
    let mut remote_needs = Vec::new();
    for (remote_author, remote_author_logs) in remote_log_heights {

        // Helper for diffing local log heights against remote log heights.
        let diff_logs = |(local_log_id, local_seq_num): (LogId, SeqNum)| {
            // Get the remote log by it's id.
            let remote_log = remote_author_logs
                .iter()
                .find(|(remote_log_id, _)| local_log_id == *remote_log_id);

            match remote_log {
                // If a log exists then compare heights of local and remote logs.
                Some((log_id, remote_seq_num)) => {
                    // If the local log is higher we increment their log id (we want all entries
                    // greater than or equal to this). Otherwise we return none.
                    if local_seq_num > *remote_seq_num {
                        // We can unwrap as we are incrementing the remote peers seq num here
                        // and this means it's will not reach max seq number.
                        Some((log_id.to_owned(), remote_seq_num.clone().next().unwrap()))
                    } else {
                        None
                    }
                }
                // If no log exists then the remote has never had this log and they need
                // all entries from seq num 1.
                None => Some((local_log_id.to_owned(), SeqNum::default())),
            }
        };

        // Find local log for a public key sent by the remote peer.
        //
        // If none is found we don't do anything as this means we are missing entries they should send us.
        if let Some((_, local_author_logs)) = local_log_heights
            .iter()
            .find(|(local_author, _)| local_author == remote_author)
        {
            // Diff our local log heights against the remote.
            let remote_needs_logs: Vec<(LogId, SeqNum)> = local_author_logs
                .to_owned()
                .into_iter()
                .filter_map(diff_logs)
                .collect();

            // If the remote needs at least one log we push it to the remote needs.
            if !remote_needs_logs.is_empty() {
                remote_needs.push((remote_author.to_owned(), remote_needs_logs));
            };
        }
    }
    remote_needs
}

#[derive(Clone, Debug)]
pub struct NaiveStrategy {
    target_set: TargetSet,
    received_remote_have: bool,
    sent_have: bool,
}

impl NaiveStrategy {
    pub fn new(target_set: &TargetSet) -> Self {
        Self {
            target_set: target_set.clone(),
            received_remote_have: false,
            sent_have: false,
        }
    }

    async fn local_log_heights(&self, store: &SqlStore) -> Vec<LogHeight> {
        let mut result = vec![];

        // For every schema id in the target set retrieve log heights for all contributing authors
        for schema_id in self.target_set().0.iter() {
            let log_heights = store
                .get_log_heights(schema_id)
                .await
                .expect("Fatal database error");
            result.extend(log_heights);
        }

        result
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
        vec![Message::Have(self.local_log_heights(store).await)]
    }

    async fn handle_message(
        &mut self,
        store: &SqlStore,
        message: &Message,
    ) -> Result<StrategyResult, ReplicationError> {
        let mut messages = Vec::new();
        let mut is_local_done = false;

        // Send our Have message to remote if we haven't done it yet
        if !self.sent_have {
            messages.extend(self.initial_messages(store).await);
            self.sent_have = true;
        }

        match message {
            Message::Have(remote_log_heights) => {
                if self.received_remote_have {
                    return Err(ReplicationError::StrategyFailed(
                        "Received Have from remote message twice".into(),
                    ));
                }

                let local_log_heights = self.local_log_heights(&store).await;
                let remote_needs = diff_log_heights(&local_log_heights, remote_log_heights);

                for (public_key, log_heights) in remote_needs {
                    for (log_id, seq_num) in log_heights {
                        let entry_messages: Vec<Message> = store
                            .get_entries_from(&public_key, &log_id, &seq_num)
                            .await
                            .expect("Fatal database error")
                            .iter()
                            .map(|entry| {
                                Message::Entry(
                                    entry.clone().encoded_entry,
                                    entry.payload().cloned(),
                                )
                            })
                            .collect();
                        messages.extend(entry_messages);
                    }
                }

                is_local_done = true;
                self.received_remote_have = true;
            }
            _ => {
                return Err(ReplicationError::StrategyFailed(
                    "Received unknown message type".into(),
                ));
            }
        }

        Ok(StrategyResult {
            is_local_done,
            messages,
        })
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
    fn correctly_diffs_log_heights(#[from(random_key_pair)] author_a: KeyPair) {
        let author_a = author_a.public_key();
        let peer_a_log_heights = vec![(
            author_a,
            vec![
                (LogId::new(0), SeqNum::new(5).unwrap()),
                (LogId::new(1), SeqNum::new(2).unwrap()),
            ],
        )];
        let peer_b_log_heights = vec![(
            author_a,
            vec![
                (LogId::new(0), SeqNum::new(8).unwrap()),
                (LogId::new(1), SeqNum::new(2).unwrap()),
            ],
        )];

        let peer_b_needs = diff_log_heights(&peer_a_log_heights, &peer_b_log_heights);
        let peer_a_needs = diff_log_heights(&peer_b_log_heights, &peer_a_log_heights);

        assert_eq!(
            peer_a_needs,
            vec![(author_a, vec![(LogId::new(0), SeqNum::new(6).unwrap())])]
        );
        assert_eq!(peer_b_needs, vec![]);
    }
}
