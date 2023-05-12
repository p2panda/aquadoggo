// SPDX-License-Identifier: AGPL-3.0-or-later

use asynchronous_codec::CborCodec;
use serde::{Deserialize, Serialize};

pub const PROTOCOL_NAME: &[u8] = b"/p2p/p2panda/1.0.0";

pub type Codec = CborCodec<Message, Message>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Message {
    Dummy(u64),
}
