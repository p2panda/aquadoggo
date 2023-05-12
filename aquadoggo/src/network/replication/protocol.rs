// SPDX-License-Identifier: AGPL-3.0-or-later

use std::pin::Pin;

use asynchronous_codec::{CborCodec, CborCodecError, Framed};
use futures::{future, AsyncRead, AsyncWrite, Future};
use libp2p::core::UpgradeInfo;
use libp2p::{InboundUpgrade, OutboundUpgrade};
use serde::{Deserialize, Serialize};

pub const PROTOCOL_NAME: &[u8] = b"/p2p/p2panda/1.0.0";

pub type CodecError = CborCodecError;

pub type Codec = CborCodec<Message, Message>;

// @TODO: Get this from our other replication module
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Message {
    Dummy(u64),
}

#[derive(Clone, Debug)]
pub struct Protocol;

impl Protocol {
    pub fn new() -> Self {
        Self
    }
}

impl UpgradeInfo for Protocol {
    type Info = Vec<u8>;
    type InfoIter = Vec<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        vec![PROTOCOL_NAME.to_vec()]
    }
}

impl<TSocket> InboundUpgrade<TSocket> for Protocol
where
    TSocket: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Output = Framed<TSocket, Codec>;
    type Error = CodecError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    fn upgrade_inbound(self, socket: TSocket, _protocol_id: Self::Info) -> Self::Future {
        Box::pin(future::ok(Framed::new(socket, Codec::new())))
    }
}

impl<TSocket> OutboundUpgrade<TSocket> for Protocol
where
    TSocket: AsyncWrite + AsyncRead + Unpin + Send + 'static,
{
    type Output = Framed<TSocket, Codec>;
    type Error = CodecError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    fn upgrade_outbound(self, socket: TSocket, _protocol_id: Self::Info) -> Self::Future {
        Box::pin(future::ok(Framed::new(socket, Codec::new())))
    }
}
