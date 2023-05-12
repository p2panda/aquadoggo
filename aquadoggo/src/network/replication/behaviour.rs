// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::VecDeque;
use std::task::{Context, Poll};

use libp2p::core::Endpoint;
use libp2p::swarm::{
    ConnectionDenied, ConnectionId, FromSwarm, NetworkBehaviour, NotifyHandler, PollParameters,
    THandler, THandlerInEvent, THandlerOutEvent, ToSwarm,
};
use libp2p::{Multiaddr, PeerId};

use crate::network::replication::handler::{Handler, HandlerInEvent, HandlerOutEvent};
use crate::network::replication::protocol::Message;

#[derive(Debug)]
pub enum BehaviourOutEvent {}

#[derive(Debug)]
pub struct Behaviour {
    events: VecDeque<ToSwarm<BehaviourOutEvent, HandlerInEvent>>,
}

impl Behaviour {
    pub fn new() -> Self {
        Self {
            events: VecDeque::new(),
        }
    }
}

impl Behaviour {
    fn send_message(&mut self, peer_id: PeerId, message: Message) {
        self.events.push_back(ToSwarm::NotifyHandler {
            peer_id,
            event: HandlerInEvent::Message(message),
            handler: NotifyHandler::Any,
        });
    }

    fn handle_received_message(&self, _peer_id: &PeerId, _message: Message) {
        // @TODO: Handle incoming messages
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = Handler;

    type OutEvent = BehaviourOutEvent;

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

    fn on_connection_handler_event(
        &mut self,
        peer: PeerId,
        _connection_id: ConnectionId,
        handler_event: THandlerOutEvent<Self>,
    ) {
        match handler_event {
            HandlerOutEvent::Message(message) => {
                self.handle_received_message(&peer, message);
            }
        }
    }

    fn on_swarm_event(&mut self, event: FromSwarm<Self::ConnectionHandler>) {
        match event {
            FromSwarm::ConnectionEstablished(_)
            | FromSwarm::ConnectionClosed(_)
            | FromSwarm::AddressChange(_)
            | FromSwarm::DialFailure(_)
            | FromSwarm::ListenFailure(_)
            | FromSwarm::NewListener(_)
            | FromSwarm::NewListenAddr(_)
            | FromSwarm::ExpiredListenAddr(_)
            | FromSwarm::ListenerError(_)
            | FromSwarm::ListenerClosed(_)
            | FromSwarm::NewExternalAddr(_)
            | FromSwarm::ExpiredExternalAddr(_) => {}
        }
    }

    fn poll(
        &mut self,
        _cx: &mut Context<'_>,
        _params: &mut impl PollParameters,
    ) -> Poll<ToSwarm<Self::OutEvent, THandlerInEvent<Self>>> {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }

        Poll::Pending
    }
}
