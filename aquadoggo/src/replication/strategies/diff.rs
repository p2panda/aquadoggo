// SPDX-License-Identifier: AGPL-3.0-or-later

use log::debug;
use p2panda_rs::{
    entry::{LogId, SeqNum},
    Human,
};

use crate::replication::LogHeight;

pub fn diff_log_heights(
    local_log_heights: &[LogHeight],
    remote_log_heights: &[LogHeight],
) -> Vec<LogHeight> {
    let mut remote_needs = Vec::new();

    for (local_author, local_author_logs) in local_log_heights {
        debug!(
            "Local log heights: {} {:?}",
            local_author.display(),
            local_author_logs
        );

        // Helper for diffing local log heights against remote log heights.
        let diff_logs = |(remote_log_id, remote_seq_num): (LogId, SeqNum)| {
            debug!(
                "Remote log height: {:?} {:?}",
                remote_log_id, remote_seq_num
            );
            // Get the remote log by it's id.
            let local_log = local_author_logs
                .iter()
                .find(|(local_log_id, _)| remote_log_id == *local_log_id);

            match local_log {
                // If a log exists then compare heights of local and remote logs.
                Some((log_id, local_seq_num)) => {
                    debug!("Local log height: {:?} {:?}", log_id, local_seq_num);

                    // If the local log is higher we increment their log id (we want all entries
                    // greater than or equal to this). Otherwise we return none.
                    if local_seq_num > &remote_seq_num {
                        // We can unwrap as we are incrementing the remote peers seq num here and
                        // this means it's will not reach max seq number.
                        let next_seq_num = remote_seq_num.clone().next().unwrap();

                        debug!(
                            "Remote needs entries from {:?} - {:?} for {:?}",
                            local_seq_num, next_seq_num, log_id
                        );

                        Some((log_id.to_owned(), remote_seq_num.clone().next().unwrap()))
                    } else {
                        debug!("Remote contains all local entries");
                        None
                    }
                }
                // If no log exists then the remote has never had this log and they need all
                // entries from seq num 1.
                None => {
                    debug!("Remote needs all entries from {:?}", remote_log_id);
                    Some((remote_log_id.to_owned(), SeqNum::default()))
                }
            }
        };

        // Find local log for a public key sent by the remote peer.
        //
        // If none is found we don't do anything as this means we are missing entries they should
        // send us.
        if let Some((remote_author, remote_author_logs)) = remote_log_heights
            .iter()
            .find(|(remote_author, _)| remote_author == local_author)
        {
            debug!(
                "Remote log heights: {} {:?}",
                remote_author.display(),
                remote_author_logs
            );

            // Diff our local log heights against the remote.
            let remote_needs_logs: Vec<(LogId, SeqNum)> = remote_author_logs
                .iter()
                .copied()
                .filter_map(diff_logs)
                .collect();

            // If the remote needs at least one log we push it to the remote needs.
            if !remote_needs_logs.is_empty() {
                remote_needs.push((local_author.to_owned(), remote_needs_logs));
            };
        } else {
            remote_needs.push((
                local_author.to_owned(),
                local_author_logs
                    .iter()
                    .map(|(log_id, _)| (*log_id, SeqNum::default()))
                    .collect(),
            ));
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

    #[rstest]
    fn diff_when_remote_is_empty(#[from(random_key_pair)] author_a: KeyPair) {
        let author_a = author_a.public_key();
        let peer_a_log_heights = vec![(
            author_a,
            vec![
                (LogId::new(0), SeqNum::new(5).unwrap()),
                (LogId::new(1), SeqNum::new(5).unwrap()),
            ],
        )];
        let peer_b_log_heights = vec![];

        let peer_b_needs = diff_log_heights(&peer_a_log_heights, &peer_b_log_heights);
        let peer_a_needs = diff_log_heights(&peer_b_log_heights, &peer_a_log_heights);

        assert_eq!(peer_a_needs, vec![]);
        assert_eq!(
            peer_b_needs,
            vec![(
                author_a,
                vec![
                    (LogId::new(0), SeqNum::new(1).unwrap()),
                    (LogId::new(1), SeqNum::new(1).unwrap())
                ]
            ),]
        );
    }
}
