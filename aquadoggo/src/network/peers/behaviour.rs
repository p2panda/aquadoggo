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
use log::trace;
use p2panda_rs::Human;

use crate::network::peers::handler::{Handler, HandlerFromBehaviour, HandlerToBehaviour};
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

/// p2panda network behaviour managing peers who can speak the "p2panda" protocol, handling
/// incoming and outgoing messages related to it.
///
/// This custom behaviour represents the "p2panda" protocol. As soon as both peers agree that they
/// can speak the "p2panda" protocol libp2p will upgrade the connection and enable this custom
/// `NetworkBehaviour` implementation.
///
/// All behaviours will share the same connections but each individual behaviour maintains its own
/// connection handlers on top of them. With this in mind the following procedure takes place:
///
/// 1. Swarm discovers a node and dials a new outgoing connection OR swarm listener was dialed from
///    a remote peer, establishes a new incoming connection
/// 2. Swarm negotiates if new node can speak the "p2panda" protocol. If this is the case the
///    connection gets upgraded
/// 3. Custom p2panda `NetworkBehaviour` initialises the `ConnectionHandler` for the underlying
///    connection (see `handle_established_inbound_connection` or
///    `handle_established_outbound_connection`) and informs other services about new peer
/// 4. Custom p2panda `ConnectionHandler` establishes bi-directional streams which encode and
///    decode CBOR messages for us. As soon as a new message arrives the handler informs the
///    behaviour about it
/// 5. Custom p2panda `NetworkBehaviour` receives incoming message from handler and passes it
///    further to other services
/// 6. Custom p2panda `NetworkBehaviour` receives messages from other services and passes them down
///    again to `ConnectionHandler` which sends them over the data stream to remote node
/// 7. Swarm informs `NetworkBehaviour` about closed connection handlers (gracefully or via
///    time-out). The custom p2panda `NetworkBehaviour` informs other services about disconnected
///    peer
///
/// ```text
///           Swarm
///          ┌──────────────────────────────────────────────────────────────────┐
///          │  ┌──────────────┐       ┌──────────────┐       ┌──────────────┐  │
///          │  │  Connection  │       │  Connection  │       │  Connection  │  │
///          │  └──────┬───────┘       └───────┬──────┘       └───────┬──────┘  │
///          │         │                       │                      │         │
///          │      Upgrade                 Upgrade                Upgrade      │
///          │         │                       │                      │         │
///          └─────────┼───────────────────────┼──────────────────────┼─────────┘
///                    │                       │                      │
///    ┌───────────────┼───────────────────────┼──────────────────────┼────────────────┐
///    │    ┌──────────┴───────────────────────┴──────────────────────┴───────────┐    │
///    │    │                          NetworkBehaviour                           │    │
///    │    └──────────┬───────────────────────┬──────────────────────┬───────────┘    │
///    │               │                       │                      │                │
///    │    ┌──────────▼──────────┐ ┌──────────▼──────────┐ ┌─────────▼───────────┐    │
///    │    │  ConnectionHandler  │ │  ConnectionHandler  │ │  ConnectionHandler  │    │
///    │    └─────────────────────┘ └─────────────────────┘ └─────────────────────┘    │
///    └───────────────────────────────────────────────────────────────────────────────┘
///     p2panda protocol
/// ```
#[derive(Debug)]
pub struct Behaviour {
    events: VecDeque<ToSwarm<Event, HandlerFromBehaviour>>,
    running: bool,
}

impl Behaviour {
    pub fn new() -> Self {
        Self {
            events: VecDeque::new(),
            running: false,
        }
    }

    fn on_connection_established(&mut self, peer_id: PeerId, connection_id: ConnectionId) {
        let peer = Peer::new(peer_id, connection_id);
        self.events
            .push_back(ToSwarm::GenerateEvent(Event::PeerConnected(peer)));
    }

    fn on_connection_closed(&mut self, peer_id: PeerId, connection_id: ConnectionId) {
        let peer = Peer::new(peer_id, connection_id);
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
            "Notify swarm of received sync message: {} {}",
            peer.display(),
            message.display()
        );
        self.events
            .push_back(ToSwarm::GenerateEvent(Event::MessageReceived(
                peer, message,
            )));
    }

    pub fn run(&mut self) {
        self.running = true
    }

    pub fn send_message(&mut self, peer: Peer, message: SyncMessage) {
        trace!(
            "Notify handler of sent sync message: {} {}",
            peer.display(),
            message.display(),
        );
        self.events.push_back(ToSwarm::NotifyHandler {
            peer_id: peer.id(),
            event: HandlerFromBehaviour::Message(message),
            handler: NotifyHandler::One(peer.connection_id()),
        });
    }

    pub fn handle_critical_error(&mut self, peer: Peer) {
        self.events.push_back(ToSwarm::NotifyHandler {
            peer_id: peer.id(),
            event: HandlerFromBehaviour::CriticalError,
            handler: NotifyHandler::One(peer.connection_id()),
        });
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = Handler;

    type ToSwarm = Event;

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
            HandlerToBehaviour::Message(message) => {
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
            | FromSwarm::NewExternalAddrCandidate(_)
            | FromSwarm::ExternalAddrConfirmed(_)
            | FromSwarm::ExternalAddrExpired(_) => {}
        }
    }

    fn poll(
        &mut self,
        _cx: &mut Context<'_>,
        _params: &mut impl PollParameters,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if self.running {
            if let Some(event) = self.events.pop_front() {
                return Poll::Ready(event);
            }
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use libp2p::swarm::{keep_alive, ConnectionId, Swarm};
    use libp2p_swarm_test::SwarmExt;
    use p2panda_rs::schema::SchemaId;
    use rstest::rstest;

    use crate::network::Peer;
    use crate::replication::{Message, SyncMessage, TargetSet};
    use crate::test_utils::helpers::random_target_set;

    use super::{Behaviour as PeersBehaviour, Event};

    #[tokio::test]
    async fn peers_connect() {
        // Create two swarms
        let mut swarm_1 = Swarm::new_ephemeral(|_| PeersBehaviour::new());
        let mut swarm_2 = Swarm::new_ephemeral(|_| PeersBehaviour::new());

        // Listen on swarm_1 and connect from swarm_2, this should establish a bi-directional
        // connection.
        swarm_1.listen().await;
        swarm_2.connect(&mut swarm_1).await;

        let swarm_1_peer_id = *swarm_1.local_peer_id();
        let swarm_2_peer_id = *swarm_2.local_peer_id();

        let info1 = swarm_1.network_info();
        let info2 = swarm_2.network_info();

        // Peers should be connected.
        assert!(swarm_2.is_connected(&swarm_1_peer_id));
        assert!(swarm_1.is_connected(&swarm_2_peer_id));

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
        let mut swarm_1 = Swarm::new_ephemeral(|_| PeersBehaviour::new());
        let mut swarm_2 = Swarm::new_ephemeral(|_| keep_alive::Behaviour);

        // Listen on swarm_1 and connect from swarm_2, this should establish a bi-directional connection.
        swarm_1.listen().await;
        swarm_2.connect(&mut swarm_1).await;

        let swarm_1_peer_id = *swarm_1.local_peer_id();
        let swarm_2_peer_id = *swarm_2.local_peer_id();

        let info1 = swarm_1.network_info();
        let info2 = swarm_2.network_info();

        // Even though the network behaviours of our two peers are incompatible they still
        // establish a connection.

        // Peers should be connected.
        assert!(swarm_2.is_connected(&swarm_1_peer_id));
        assert!(swarm_1.is_connected(&swarm_2_peer_id));

        // Each swarm should have exactly one connected peer.
        assert_eq!(info1.num_peers(), 1);
        assert_eq!(info2.num_peers(), 1);

        // Each swarm should have one established connection.
        assert_eq!(info1.connection_counters().num_established(), 1);
        assert_eq!(info2.connection_counters().num_established(), 1);

        // Send a message from to swarm_1 local peer from swarm_2 local peer.
        swarm_1.behaviour_mut().send_message(
            Peer::new(swarm_2_peer_id, ConnectionId::new_unchecked(1)),
            SyncMessage::new(0, Message::SyncRequest(0.into(), TargetSet::new(&vec![]))),
        );

        // Await a swarm event on swarm_2.
        //
        // We expect a timeout panic as no event will occur.
        let result = std::panic::AssertUnwindSafe(swarm_2.next_swarm_event())
            .catch_unwind()
            .await;

        assert!(result.is_err())
    }

    #[rstest]
    #[case(
        TargetSet::new(&vec![SchemaId::SchemaFieldDefinition(0)]),
        TargetSet::new(&vec![SchemaId::SchemaDefinition(0)]),
    )]
    #[case(random_target_set(), random_target_set())]
    #[tokio::test]
    async fn swarm_behaviour_events(
        #[case] target_set_1: TargetSet,
        #[case] target_set_2: TargetSet,
    ) {
        let mut swarm_1 = Swarm::new_ephemeral(|_| PeersBehaviour::new());
        let mut swarm_2 = Swarm::new_ephemeral(|_| PeersBehaviour::new());

        // Listen on swarm_1 and connect from swarm_2, this should establish a bi-directional
        // connection
        swarm_1.listen().await;
        swarm_2.connect(&mut swarm_1).await;

        let mut events_1 = Vec::new();
        let mut events_2 = Vec::new();

        let swarm_1_peer_id = *swarm_1.local_peer_id();
        let swarm_2_peer_id = *swarm_2.local_peer_id();

        // Collect the next 2 behaviour events which occur in either swarms.
        for _ in 0..2 {
            tokio::select! {
                Event::PeerConnected(peer) = swarm_1.next_behaviour_event() => {
                    events_1.push((peer, None));
                },
                Event::PeerConnected(peer) = swarm_2.next_behaviour_event() => events_2.push((peer, None)),
            }
        }

        assert_eq!(events_1.len(), 1);
        assert_eq!(events_2.len(), 1);

        // The first event should have been a ConnectionEstablished containing the expected peer
        // id
        let (peer_2, message) = events_1[0].clone();
        assert_eq!(peer_2.id(), swarm_2_peer_id);
        assert!(message.is_none());

        let (peer_1, message) = events_2[0].clone();
        assert_eq!(peer_1.id(), swarm_1_peer_id);
        assert!(message.is_none());

        // Send a message from swarm_1 to swarm_2
        swarm_1.behaviour_mut().send_message(
            peer_2,
            SyncMessage::new(0, Message::SyncRequest(0.into(), target_set_1.clone())),
        );

        // Send a message from swarm_2 to swarm_1
        swarm_2.behaviour_mut().send_message(
            peer_1,
            SyncMessage::new(1, Message::SyncRequest(0.into(), target_set_2.clone())),
        );

        // And again add the next behaviour events which occur in either swarms
        for _ in 0..2 {
            tokio::select! {
                Event::MessageReceived(peer, message) = swarm_1.next_behaviour_event() => events_1.push((peer, Some(message))),
                Event::MessageReceived(peer, message) = swarm_2.next_behaviour_event() => events_2.push((peer, Some(message))),
            }
        }

        assert_eq!(events_1.len(), 2);
        assert_eq!(events_2.len(), 2);

        // swarm_1 should have received the message from swarm_2 peer
        let (peer, message) = events_1[1].clone();
        assert_eq!(peer.id(), swarm_2_peer_id);
        assert_eq!(
            message.unwrap(),
            SyncMessage::new(1, Message::SyncRequest(0.into(), target_set_2.clone()))
        );

        // swarm_2 should have received the message from swarm_1 peer
        let (peer, message) = events_2[1].clone();
        assert_eq!(peer.id(), swarm_1_peer_id);
        assert_eq!(
            message.unwrap(),
            SyncMessage::new(0, Message::SyncRequest(0.into(), target_set_1))
        );
    }
}
