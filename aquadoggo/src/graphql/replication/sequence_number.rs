use async_graphql::*;
use p2panda_rs::entry::{SeqNum as PandaSeqNum, SeqNumError};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

/// The sequence number of an entry
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct SequenceNumber(pub PandaSeqNum);

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
