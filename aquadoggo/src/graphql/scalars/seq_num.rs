// SPDX-License-Identifier: AGPL-3.0-or-later

use async_graphql::scalar;
use serde::{Deserialize, Serialize};

/// Sequence number of an entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SeqNum(p2panda_rs::entry::SeqNum);

impl From<p2panda_rs::entry::SeqNum> for SeqNum {
    fn from(seq_num: p2panda_rs::entry::SeqNum) -> Self {
        Self(seq_num)
    }
}

impl From<SeqNum> for p2panda_rs::entry::SeqNum {
    fn from(seq_num: SeqNum) -> p2panda_rs::entry::SeqNum {
        seq_num.0
    }
}

scalar!(SeqNum);
