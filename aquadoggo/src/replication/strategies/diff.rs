// SPDX-License-Identifier: AGPL-3.0-or-later

use p2panda_rs::entry::{LogId, SeqNum};

use crate::replication::LogHeight;

pub fn diff_log_heights(
    local_log_heights: &[LogHeight],
    remote_log_heights: &[LogHeight],
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
                        // We can unwrap as we are incrementing the remote peers seq num here and
                        // this means it's will not reach max seq number.
                        Some((log_id.to_owned(), remote_seq_num.clone().next().unwrap()))
                    } else {
                        None
                    }
                }
                // If no log exists then the remote has never had this log and they need all
                // entries from seq num 1.
                None => Some((local_log_id.to_owned(), SeqNum::default())),
            }
        };

        // Find local log for a public key sent by the remote peer.
        //
        // If none is found we don't do anything as this means we are missing entries they should
        // send us.
        if let Some((_, local_author_logs)) = local_log_heights
            .iter()
            .find(|(local_author, _)| local_author == remote_author)
        {
            // Diff our local log heights against the remote.
            let remote_needs_logs: Vec<(LogId, SeqNum)> = local_author_logs
                .iter()
                .copied()
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
