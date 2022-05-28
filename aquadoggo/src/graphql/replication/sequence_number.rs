// SPDX-License-Identifier: AGPL-3.0-or-later

use std::convert::TryFrom;

use async_graphql::*;
use anyhow::Result;
use p2panda_rs::entry::{SeqNum as PandaSeqNum, SeqNumError};
use serde::{Deserialize, Serialize};

/// The sequence number of an entry
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct SequenceNumber(pub PandaSeqNum);

impl SequenceNumber{
    pub fn new(seq: u64) -> Result<Self>{
        let panda_seq_num = PandaSeqNum::new(seq)?;
        Ok(Self(panda_seq_num))
    }
}

impl SequenceNumber {
    pub fn as_u64(self) -> u64 {
        self.0.as_u64()
    }
}

impl AsRef<PandaSeqNum> for SequenceNumber {
    fn as_ref(&self) -> &PandaSeqNum {
        &self.0
    }
}

impl TryFrom<u64> for SequenceNumber {
    type Error = SeqNumError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        let seq_num = PandaSeqNum::new(value)?;
        Ok(Self(seq_num))
    }
}

scalar!(SequenceNumber);
