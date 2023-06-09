// SPDX-License-Identifier: AGPL-3.0-or-later

use std::pin::Pin;

use asynchronous_codec::{CborCodec, CborCodecError, Framed};
use futures::{future, AsyncRead, AsyncWrite, Future};
use libp2p::core::UpgradeInfo;
use libp2p::{InboundUpgrade, OutboundUpgrade};

use crate::replication::SyncMessage;

pub const PROTOCOL_NAME: &str = "/p2p/p2panda/1.0.0";

pub type CodecError = CborCodecError;

pub type Codec = CborCodec<SyncMessage, SyncMessage>;

#[derive(Clone, Debug)]
pub struct Protocol;

impl Protocol {
    pub fn new() -> Self {
        Self
    }
}

impl UpgradeInfo for Protocol {
    type Info = String;
    type InfoIter = Vec<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        vec![PROTOCOL_NAME.to_string()]
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
