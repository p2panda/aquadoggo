// SPDX-License-Identifier: AGPL-3.0-or-later

use std::task::{Context, Poll};

use libp2p::core::Endpoint;
use libp2p::swarm::{ConnectionDenied, ConnectionId, NetworkBehaviour, THandler, THandlerOutEvent};
use libp2p::{Multiaddr, PeerId};

use crate::network::replication::Handler;

#[derive(Debug)]
pub struct Behaviour;

#[derive(Debug)]
pub struct Event;

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = Handler;

    type OutEvent = Event;

    fn handle_established_inbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(Handler::new())
    }

    fn handle_established_outbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: Endpoint,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(Handler::new())
    }
    fn on_swarm_event(&mut self, _event: libp2p::swarm::FromSwarm<Self::ConnectionHandler>) {
        todo!()
    }

    fn on_connection_handler_event(
        &mut self,
        _peer: PeerId,
        _: ConnectionId,
        _result: THandlerOutEvent<Self>,
    ) {
        todo!()
    }

    fn poll(
        &mut self,
        _cx: &mut Context<'_>,
        _params: &mut impl libp2p::swarm::PollParameters,
    ) -> Poll<libp2p::swarm::ToSwarm<Self::OutEvent, libp2p::swarm::THandlerInEvent<Self>>> {
        todo!()
    }
}
