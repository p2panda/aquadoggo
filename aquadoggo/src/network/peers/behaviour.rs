// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::VecDeque;
use std::task::{Context, Poll};

use libp2p::core::Endpoint;
use libp2p::swarm::derive_prelude::ConnectionEstablished;
use libp2p::swarm::{
    ConnectionClosed, ConnectionDenied, ConnectionId, FromSwarm, NetworkBehaviour, NotifyHandler,
    PollParameters, THandler, THandlerInEvent, THandlerOutEvent, ToSwarm,
};
use libp2p::{Multiaddr, PeerId};
use log::{info, trace};
use p2panda_rs::Human;

use crate::network::peers::handler::{Handler, HandlerInEvent, HandlerOutEvent};
use crate::network::peers::Peer;
use crate::replication::SyncMessage;

#[derive(Debug)]
pub enum Event {
    /// Message received on the inbound stream.
    MessageReceived(Peer, SyncMessage),

    /// We established an inbound or outbound connection to a peer for the first time.
    PeerConnected(Peer),

    /// Peer does not have any inbound or outbound connections left with us.
    PeerDisconnected(Peer),
}

#[derive(Debug)]
pub struct Behaviour {
    events: VecDeque<ToSwarm<Event, HandlerInEvent>>,
}

impl Behaviour {
    pub fn new() -> Self {
        Self {
            events: VecDeque::new(),
        }
    }

    fn on_connection_established(&mut self, peer_id: PeerId, connection_id: ConnectionId) {
        let peer = Peer::new(peer_id, connection_id);
        info!("New peer connected: {peer}");
        self.events
            .push_back(ToSwarm::GenerateEvent(Event::PeerConnected(peer)));
    }

    fn on_connection_closed(&mut self, peer_id: PeerId, connection_id: ConnectionId) {
        let peer = Peer::new(peer_id, connection_id);
        info!("Peer disconnected: {peer}");
        self.events
            .push_back(ToSwarm::GenerateEvent(Event::PeerDisconnected(peer)));
    }

    fn on_received_message(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        message: SyncMessage,
    ) {
        let peer = Peer::new(peer_id, connection_id);
        trace!(
            "Notify swarm of received sync message: {peer} {}",
            message.display()
        );
        self.events
            .push_back(ToSwarm::GenerateEvent(Event::MessageReceived(
                peer, message,
            )));
    }

    pub fn send_message(&mut self, peer: Peer, message: SyncMessage) {
        trace!(
            "Notify handler of sent sync message: {peer} {}",
            message.display()
        );
        self.events.push_back(ToSwarm::NotifyHandler {
            peer_id: peer.id(),
            event: HandlerInEvent::Message(message),
            handler: NotifyHandler::One(peer.connection_id()),
        });
    }

    pub fn handle_critical_error(&mut self, peer: Peer) {
        self.events.push_back(ToSwarm::NotifyHandler {
            peer_id: peer.id(),
            event: HandlerInEvent::CriticalError,
            handler: NotifyHandler::One(peer.connection_id()),
        });
    }
}

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

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        handler_event: THandlerOutEvent<Self>,
    ) {
        match handler_event {
            HandlerOutEvent::Message(message) => {
                self.on_received_message(peer_id, connection_id, message);
            }
        }
    }

    fn on_swarm_event(&mut self, event: FromSwarm<Self::ConnectionHandler>) {
        match event {
            FromSwarm::ConnectionEstablished(ConnectionEstablished {
                peer_id,
                connection_id,
                ..
            }) => {
                self.on_connection_established(peer_id, connection_id);
            }
            FromSwarm::ConnectionClosed(ConnectionClosed {
                peer_id,
                connection_id,
                ..
            }) => {
                self.on_connection_closed(peer_id, connection_id);
            }
            FromSwarm::AddressChange(_)
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

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use libp2p::swarm::{keep_alive, Swarm};
    use libp2p_swarm_test::SwarmExt;
    use p2panda_rs::schema::SchemaId;
    use rstest::rstest;

    use crate::replication::{Message, SyncMessage, TargetSet};
    use crate::test_utils::helpers::random_target_set;

    use super::{Behaviour as PeersBehaviour, Event};

    #[tokio::test]
    async fn peers_connect() {
        // Create two swarms
        let mut swarm1 = Swarm::new_ephemeral(|_| PeersBehaviour::new());
        let mut swarm2 = Swarm::new_ephemeral(|_| PeersBehaviour::new());

        // Listen on swarm1 and connect from swarm2, this should establish a bi-directional
        // connection.
        swarm1.listen().await;
        swarm2.connect(&mut swarm1).await;

        let swarm1_peer_id = *swarm1.local_peer_id();
        let swarm2_peer_id = *swarm2.local_peer_id();

        let info1 = swarm1.network_info();
        let info2 = swarm2.network_info();

        // Peers should be connected.
        assert!(swarm2.is_connected(&swarm1_peer_id));
        assert!(swarm1.is_connected(&swarm2_peer_id));

        // Each swarm should have exactly one connected peer.
        assert_eq!(info1.num_peers(), 1);
        assert_eq!(info2.num_peers(), 1);

        // Each swarm should have one established connection.
        assert_eq!(info1.connection_counters().num_established(), 1);
        assert_eq!(info2.connection_counters().num_established(), 1);
    }

    #[tokio::test]
    async fn incompatible_network_behaviour() {
        // Create two swarms
        let mut swarm1 = Swarm::new_ephemeral(|_| PeersBehaviour::new());
        let mut swarm2 = Swarm::new_ephemeral(|_| keep_alive::Behaviour);

        // Listen on swarm1 and connect from swarm2, this should establish a bi-directional connection.
        swarm1.listen().await;
        swarm2.connect(&mut swarm1).await;

        let swarm1_peer_id = *swarm1.local_peer_id();
        let swarm2_peer_id = *swarm2.local_peer_id();

        let info1 = swarm1.network_info();
        let info2 = swarm2.network_info();

        // Even though the network behaviours of our two peers are incompatible they still
        // establish a connection.

        // Peers should be connected.
        assert!(swarm2.is_connected(&swarm1_peer_id));
        assert!(swarm1.is_connected(&swarm2_peer_id));

        // Each swarm should have exactly one connected peer.
        assert_eq!(info1.num_peers(), 1);
        assert_eq!(info2.num_peers(), 1);

        // Each swarm should have one established connection.
        assert_eq!(info1.connection_counters().num_established(), 1);
        assert_eq!(info2.connection_counters().num_established(), 1);

        // Send a message from to swarm1 local peer from swarm2 local peer.
        swarm1.behaviour_mut().send_message(
            swarm2_peer_id,
            SyncMessage::new(0, Message::SyncRequest(0.into(), TargetSet::new(&vec![]))),
        );

        // Await a swarm event on swarm2.
        //
        // We expect a timeout panic as no event will occur.
        let result = std::panic::AssertUnwindSafe(swarm2.next_swarm_event())
            .catch_unwind()
            .await;

        assert!(result.is_err())
    }

    #[rstest]
    #[case(TargetSet::new(&vec![SchemaId::SchemaFieldDefinition(0)]), TargetSet::new(&vec![SchemaId::SchemaDefinition(0)]))]
    #[case(random_target_set(), random_target_set())]
    #[tokio::test]
    async fn swarm_behaviour_events(
        #[case] target_set_1: TargetSet,
        #[case] target_set_2: TargetSet,
    ) {
        // Create two swarms
        let mut swarm1 = Swarm::new_ephemeral(|_| PeersBehaviour::new());
        let mut swarm2 = Swarm::new_ephemeral(|_| PeersBehaviour::new());

        // Listen on swarm1 and connect from swarm2, this should establish a bi-directional connection.
        swarm1.listen().await;
        swarm2.connect(&mut swarm1).await;

        let mut res1 = Vec::new();
        let mut res2 = Vec::new();

        let swarm1_peer_id = *swarm1.local_peer_id();
        let swarm2_peer_id = *swarm2.local_peer_id();

        // Send a message from swarm1 to peer2.
        swarm1.behaviour_mut().send_message(
            swarm2_peer_id,
            SyncMessage::new(0, Message::SyncRequest(0.into(), target_set_1.clone())),
        );

        // Send a message from swarm2 peer1.
        swarm2.behaviour_mut().send_message(
            swarm1_peer_id,
            SyncMessage::new(1, Message::SyncRequest(0.into(), target_set_2.clone())),
        );

        // Collect the next 2 behaviour events which occur in either swarms.
        for _ in 0..2 {
            tokio::select! {
                Event::PeerConnected(peer_id) = swarm1.next_behaviour_event() => res1.push((peer_id, None)),
                Event::PeerConnected(peer_id) = swarm2.next_behaviour_event() => res2.push((peer_id, None)),
            }
        }

        // And again add the next 2 behaviour events which occur in either swarms.
        for _ in 0..2 {
            tokio::select! {
                Event::MessageReceived(peer_id, message) = swarm1.next_behaviour_event() => res1.push((peer_id, Some(message))),
                Event::MessageReceived(peer_id, message) = swarm2.next_behaviour_event() => res2.push((peer_id, Some(message))),
            }
        }

        // Each swarm should have emitted exactly one event.
        assert_eq!(res1.len(), 2);
        assert_eq!(res2.len(), 2);

        // The first event should have been a ConnectionEstablished containing the expected peer id.
        let (peer_id, message) = res1[0].clone();
        assert_eq!(peer_id, swarm2_peer_id);
        assert!(message.is_none());

        let (peer_id, message) = res2[0].clone();
        assert_eq!(peer_id, swarm1_peer_id);
        assert!(message.is_none());

        // swarm1 should have received the message from swarm2 peer.
        let (peer_id, message) = res1[1].clone();
        assert_eq!(peer_id, swarm2_peer_id);
        assert_eq!(
            message.unwrap(),
            SyncMessage::new(1, Message::SyncRequest(0.into(), target_set_2.clone()))
        );

        // swarm2 should have received the message from swarm1 peer.
        let (peer_id, message) = res2[1].clone();
        assert_eq!(peer_id, swarm1_peer_id);
        assert_eq!(
            message.unwrap(),
            SyncMessage::new(0, Message::SyncRequest(0.into(), target_set_1))
        );
    }
}
