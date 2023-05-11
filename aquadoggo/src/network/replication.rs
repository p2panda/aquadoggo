// SPDX-License-Identifier: AGPL-3.0-or-later

use std::task::{Context, Poll};
use thiserror::Error;

use libp2p::core::transport::MemoryTransport;
use libp2p::core::upgrade::{DeniedUpgrade, ReadyUpgrade};
use libp2p::core::Endpoint;
use libp2p::swarm::handler::{ConnectionEvent, FullyNegotiatedInbound, FullyNegotiatedOutbound};
use libp2p::swarm::{
    keep_alive, ConnectionDenied, ConnectionHandler, ConnectionHandlerEvent, ConnectionId,
    KeepAlive, NegotiatedSubstream, NetworkBehaviour, SubstreamProtocol, Swarm, SwarmEvent,
    THandler, THandlerOutEvent,
};
use libp2p::{identity, Multiaddr, PeerId};

pub const PROTOCOL_NAME: &[u8] = b"/p2p/naive-comprehensive/1.0.0";

pub struct Handler {
    /// The single long-lived outbound substream.
    outbound_substream: Option<OutboundSubstreamState>,

    /// The single long-lived inbound substream.
    inbound_substream: Option<InboundSubstreamState>,
}

impl Handler {
    pub fn new() -> Self {
        Self {
            outbound_substream: None,
            inbound_substream: None,
        }
    }
}

#[derive(Debug)]
pub struct InEvent();

#[derive(Debug)]
pub struct OutEvent();

#[derive(Debug, Error)]

pub enum HandlerError {
    #[error("Error!!")]
    Custom,
}

/// State of the inbound substream, opened either by us or by the remote.
enum InboundSubstreamState {
    /// Waiting for a message from the remote. The idle state for an inbound substream.
    WaitingInput(NegotiatedSubstream),
    /// The substream is being closed.
    Closing(NegotiatedSubstream),
    /// An error occurred during processing.
    Poisoned,
}

/// State of the outbound substream, opened either by us or by the remote.
enum OutboundSubstreamState {
    /// Waiting for the user to send a message. The idle state for an outbound substream.
    WaitingOutput(NegotiatedSubstream),
    /// Waiting to send a message to the remote.
    PendingSend(NegotiatedSubstream),
    /// Waiting to flush the substream so that the data arrives to the remote.
    PendingFlush(NegotiatedSubstream),
    /// An error occurred during processing.
    Poisoned,
}

impl ConnectionHandler for Handler {
    type InEvent = InEvent;
    type OutEvent = OutEvent;
    type Error = HandlerError;
    type InboundProtocol = ReadyUpgrade<&'static [u8]>;
    type OutboundProtocol = ReadyUpgrade<&'static [u8]>;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<ReadyUpgrade<&'static [u8]>, ()> {
        SubstreamProtocol::new(ReadyUpgrade::new(PROTOCOL_NAME), ())
    }

    fn on_behaviour_event(&mut self, v: Self::InEvent) {
        todo!()
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        KeepAlive::No
    }

    fn poll(
        &mut self,
        _: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<
            Self::OutboundProtocol,
            Self::OutboundOpenInfo,
            Self::OutEvent,
            Self::Error,
        >,
    > {
        Poll::Pending
    }

    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
        match event {
            ConnectionEvent::FullyNegotiatedInbound(FullyNegotiatedInbound {
                protocol: stream,
                ..
            }) => {
                // self.inbound = Some(protocol::recv_ping(stream).boxed());
            }
            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
                protocol: stream,
                ..
            }) => {
                // self.timer.reset(self.config.timeout);
                // self.outbound = Some(OutboundState::Ping(protocol::send_ping(stream).boxed()));
            }
            ConnectionEvent::DialUpgradeError(dial_upgrade_error) => {
                // self.on_dial_upgrade_error(dial_upgrade_error)
            }
            ConnectionEvent::AddressChange(_) | ConnectionEvent::ListenUpgradeError(_) => {}
        }
    }
}

pub struct Behaviour();

#[derive(Debug)]
pub struct Event();

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = Handler;

    type OutEvent = Event;

    fn handle_established_inbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: &Multiaddr,
    ) -> std::result::Result<THandler<Self>, ConnectionDenied> {
        Ok(Handler::new())
    }

    fn handle_established_outbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: Endpoint,
    ) -> std::result::Result<THandler<Self>, ConnectionDenied> {
        Ok(Handler::new())
    }
    fn on_swarm_event(&mut self, event: libp2p::swarm::FromSwarm<Self::ConnectionHandler>) {
        todo!()
    }

    fn on_connection_handler_event(
        &mut self,
        peer: PeerId,
        _: ConnectionId,
        result: THandlerOutEvent<Self>,
    ) {
        // self.events.push_front(Event { peer, result })
    }

    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
        params: &mut impl libp2p::swarm::PollParameters,
    ) -> std::task::Poll<libp2p::swarm::ToSwarm<Self::OutEvent, libp2p::swarm::THandlerInEvent<Self>>>
    {
        todo!()
    }
}
