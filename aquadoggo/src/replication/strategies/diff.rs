// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use log::{debug, trace};
use p2panda_rs::{
    entry::{LogId, SeqNum},
    identity::PublicKey,
    Human,
};

use crate::replication::LogHeight;

fn remote_requires_entries(
    log_id: &LogId,
    local_seq_num: &SeqNum,
    remote_log_heights: &HashMap<LogId, SeqNum>,
) -> Option<(LogId, SeqNum)> {
    trace!("Local log height: {:?} {:?}", log_id, local_seq_num);
    // Get height of the remote log by it's id.
    let remote_log_height = remote_log_heights.get(&log_id);

    match remote_log_height {
        // If a log exists then compare heights of local and remote logs.
        Some(remote_seq_num) => {
            trace!("Remote log height: {:?} {:?}", log_id, remote_seq_num);

            // If the local seq num is higher the remote needs all entries higher than
            // their max seq num for this log.
            if local_seq_num > &remote_seq_num {
                // We increment the seq num as we want it to represent an inclusive lower
                // bound.
                //
                // We can unwrap as we are incrementing the lower remote seq num which means it's
                // will not reach max seq number.
                let from_seq_num = remote_seq_num.clone().next().unwrap();

                trace!(
                    "Remote needs entries from {:?} for {:?}",
                    from_seq_num, log_id
                );

                Some((log_id.to_owned(), from_seq_num))
            } else {
                trace!("Remote has all entries for {:?}", log_id);
                None
            }
        }
        // If no log exists then the remote has a log we don't know about yet and we
        // return nothing.
        None => {
            trace!("{:?} not found on remote, all entries required", log_id);
            Some((log_id.to_owned(), SeqNum::default()))
        }
    }
}

pub fn diff_log_heights(
    local_log_heights: &HashMap<PublicKey, Vec<(LogId, SeqNum)>>,
    remote_log_heights: &HashMap<PublicKey, Vec<(LogId, SeqNum)>>,
) -> Vec<LogHeight> {
    let mut remote_needs = Vec::new();

    for (local_author, local_author_logs) in local_log_heights {
        trace!(
            "Local log heights: {} {:?}",
            local_author.display(),
            local_author_logs
        );

        let local_author_logs: HashMap<LogId, SeqNum> =
            local_author_logs.to_owned().into_iter().collect();

        // Find all logs sent by the remote for a public key we have locally.
        //
        // If none is found we know they need everything we have by this author.
        if let Some(remote_author_logs) = remote_log_heights.get(&local_author) {
            let remote_author_logs: HashMap<LogId, SeqNum> =
                remote_author_logs.to_owned().into_iter().collect();

            trace!("Remote log heights: {} {:?}", local_author.display(), {
                let mut logs = remote_author_logs
                    .clone()
                    .into_iter()
                    .collect::<Vec<(LogId, SeqNum)>>();
                logs.sort();
                logs
            });

            let mut remote_needs_logs = vec![];

            // For each log we diff the local and remote height and determine which entries, if
            // any, we should send them. 
            for (log_id, seq_num) in local_author_logs {
                if let Some(from_log_height) =
                    remote_requires_entries(&log_id, &seq_num, &remote_author_logs)
                {
                    remote_needs_logs.push(from_log_height)
                };
            }

            // Sort the log heights.
            remote_needs_logs.sort();

            // If the remote needs at least one log we push it to the remote needs.
            if !remote_needs_logs.is_empty() {
                remote_needs.push((local_author.to_owned(), remote_needs_logs));
            };
        } else {
            // The author we know about locally wasn't found on the remote log heights so they
            // need everything we have.

            trace!("No logs found on remote for this author");
            let mut remote_needs_logs: Vec<(LogId, SeqNum)> = local_author_logs
                .iter()
                .map(|(log_id, _)| (*log_id, SeqNum::default()))
                .collect();

            // Sort the log heights.
            remote_needs_logs.sort();

            remote_needs.push((local_author.to_owned(), remote_needs_logs));
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

        let peer_b_needs = diff_log_heights(
            &peer_a_log_heights.clone().into_iter().collect(),
            &peer_b_log_heights.clone().into_iter().collect(),
        );
        let peer_a_needs = diff_log_heights(
            &peer_b_log_heights.into_iter().collect(),
            &peer_a_log_heights.into_iter().collect(),
        );

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

        let peer_b_needs = diff_log_heights(
            &peer_a_log_heights.clone().into_iter().collect(),
            &peer_b_log_heights.clone().into_iter().collect(),
        );
        let peer_a_needs = diff_log_heights(
            &peer_b_log_heights.into_iter().collect(),
            &peer_a_log_heights.into_iter().collect(),
        );

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
