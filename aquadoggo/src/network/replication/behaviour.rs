// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::{HashMap, VecDeque};
use std::task::{Context, Poll};

use libp2p::core::Endpoint;
use libp2p::swarm::derive_prelude::ConnectionEstablished;
use libp2p::swarm::{
    ConnectionClosed, ConnectionDenied, ConnectionId, DialFailure, FromSwarm, ListenFailure,
    ListenerClosed, ListenerError, NetworkBehaviour, NotifyHandler, PollParameters, THandler,
    THandlerInEvent, THandlerOutEvent, ToSwarm,
};
use libp2p::{Multiaddr, PeerId};
use log::{debug, trace, warn};
use p2panda_rs::Human;

use crate::network::replication::handler::{Handler, HandlerInEvent, HandlerOutEvent};
use crate::replication::errors::ConnectionError;
use crate::replication::SyncMessage;

#[derive(Debug)]
pub enum Event {
    /// Replication message received on the inbound stream.
    MessageReceived(PeerId, SyncMessage),

    /// We established an inbound or outbound connection to a peer for the first time.
    PeerConnected(PeerId),

    /// Peer does not have any inbound or outbound connections left with us.
    PeerDisconnected(PeerId),
}

#[derive(Debug)]
pub struct Behaviour {
    events: VecDeque<ToSwarm<Event, HandlerInEvent>>,
    inbound_connections: HashMap<PeerId, ConnectionId>,
    outbound_connections: HashMap<PeerId, ConnectionId>,
}

impl Behaviour {
    pub fn new() -> Self {
        Self {
            events: VecDeque::new(),
            inbound_connections: HashMap::new(),
            outbound_connections: HashMap::new(),
        }
    }

    fn set_inbound_connection(&mut self, peer_id: PeerId, connection_id: ConnectionId) -> bool {
        if self.inbound_connections.get(&peer_id).is_some() {
            return false;
        }
        self.inbound_connections.insert(peer_id, connection_id);
        true
    }

    fn set_outbound_connection(&mut self, peer_id: PeerId, connection_id: ConnectionId) -> bool {
        if self.outbound_connections.get(&peer_id).is_some() {
            return false;
        }
        self.outbound_connections.insert(peer_id, connection_id);
        true
    }

    fn handle_received_message(&mut self, peer_id: &PeerId, message: SyncMessage) {
        trace!(
            "Notify swarm of received sync message: {peer_id} {}",
            message.display()
        );
        self.events
            .push_back(ToSwarm::GenerateEvent(Event::MessageReceived(
                *peer_id, message,
            )));
    }

    pub fn send_message(&mut self, peer_id: PeerId, message: SyncMessage) {
        trace!(
            "Notify handler of sent sync message: {peer_id} {}",
            message.display()
        );
        self.events.push_back(ToSwarm::NotifyHandler {
            peer_id,
            event: HandlerInEvent::Message(message),
            handler: NotifyHandler::Any,
        });
    }

    /// React to errors coming from the replication protocol living inside the replication service.
    pub fn handle_error(&mut self, peer_id: PeerId) {
        self.events.push_back(ToSwarm::NotifyHandler {
            peer_id,
            event: HandlerInEvent::ReplicationError,
            handler: NotifyHandler::Any,
        });
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = Handler;

    type OutEvent = Event;

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer_id: PeerId,
        _local_addr: &Multiaddr,
        remote_address: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        // We only want max one inbound connection per peer, so reject this connection if we
        // already have one assigned.
        if self.inbound_connections.get(&peer_id).is_some() {
            debug!("Connection denied: inbound connection already exists for: {peer_id}");
            return Err(ConnectionDenied::new(
                ConnectionError::MultipleInboundConnections(peer_id.to_owned()),
            ));
        }
        debug!(
            "New connection: established inbound connection with peer: {peer_id} {remote_address}"
        );
        self.set_inbound_connection(peer_id, connection_id);
        Ok(Handler::new())
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer_id: PeerId,
        _: &Multiaddr,
        _: Endpoint,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        // We only want max one outbound connection per peer, so reject this connection if we
        // already have one assigned.
        if self.outbound_connections.get(&peer_id).is_some() {
            debug!("Connection denied: outbound connection already exists for: {peer_id}");
            return Err(ConnectionDenied::new(
                ConnectionError::MultipleOutboundConnections(peer_id),
            ));
        }
        debug!("New connection: established outbound connection with peer: {peer_id}");
        self.set_outbound_connection(peer_id, connection_id);
        Ok(Handler::new())
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        handler_event: THandlerOutEvent<Self>,
    ) {
        // We only want to process messages which arrive for connections we have assigned to this peer.
        let mut current_inbound = false;
        let mut current_outbound = false;

        if let Some(inbound_connection_id) = self.inbound_connections.get(&peer_id) {
            current_inbound = *inbound_connection_id == connection_id;
        }

        if let Some(outbound_connection_id) = self.outbound_connections.get(&peer_id) {
            current_outbound = *outbound_connection_id == connection_id;
        }

        if current_inbound || current_outbound {
            match handler_event {
                HandlerOutEvent::Message(message) => {
                    self.handle_received_message(&peer_id, message);
                }
            }
        } else {
            debug!("Message ignored: message arrived on an unknown connection for: {peer_id}");
        }
    }

    fn on_swarm_event(&mut self, event: FromSwarm<Self::ConnectionHandler>) {
        match event {
            FromSwarm::ConnectionClosed(ConnectionClosed {
                peer_id,
                connection_id,
                ..
            }) => {
                let inbound = self.inbound_connections.get(&peer_id);
                let outbound = self.outbound_connections.get(&peer_id);

                match (inbound, outbound) {
                    // An inbound and outbound connection exists for this peer
                    (Some(inbound_connection_id), Some(outbound_connection_id)) => {
                        if *outbound_connection_id == connection_id {
                            debug!(
                                "Remove connections: remove outbound connection with peer: {peer_id}"
                            );
                            self.outbound_connections.remove(&peer_id);
                        }

                        if *inbound_connection_id == connection_id {
                            debug!(
                                "Remove connections: remove inbound connection with peer: {peer_id}"
                            );
                            self.inbound_connections.remove(&peer_id);
                        }
                    }
                    // Only an outbound connection exists
                    (None, Some(outbound_connection_id)) => {
                        debug!(
                            "Remove connections: remove outbound connection with peer: {peer_id}"
                        );
                        if *outbound_connection_id == connection_id {
                            self.outbound_connections.remove(&peer_id);
                            self.events
                                .push_back(ToSwarm::GenerateEvent(Event::PeerDisconnected(
                                    peer_id,
                                )));
                        }
                    }
                    // Only an inbound connection exists,
                    (Some(inbound_connection_id), None) => {
                        debug!(
                            "Remove connections: remove inbound connection with peer: {peer_id}"
                        );
                        if *inbound_connection_id == connection_id {
                            self.inbound_connections.remove(&peer_id);
                            self.events
                                .push_back(ToSwarm::GenerateEvent(Event::PeerDisconnected(
                                    peer_id,
                                )));
                        }
                    }
                    (None, None) => {
                        warn!("Attempted to disconnect a peer with no known connections");
                    }
                }
            }
            FromSwarm::ConnectionEstablished(ConnectionEstablished {
                peer_id,
                connection_id,
                ..
            }) => {
                // We only want to issue PeerConnected messages for connections we have accepted.
                let mut current_inbound = false;
                let mut current_outbound = false;

                if let Some(inbound_connection_id) = self.inbound_connections.get(&peer_id) {
                    current_inbound = *inbound_connection_id == connection_id;
                }

                if let Some(outbound_connection_id) = self.outbound_connections.get(&peer_id) {
                    current_outbound = *outbound_connection_id == connection_id;
                }

                if current_inbound || current_outbound {
                    self.events
                        .push_back(ToSwarm::GenerateEvent(Event::PeerConnected(peer_id)));
                } else {
                    warn!("Unknown connection: ignoring unknown connection with: {peer_id}");
                }
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

    use super::{Behaviour as ReplicationBehaviour, Event};

    #[tokio::test]
    async fn peers_connect() {
        // Create two swarms
        let mut swarm1 = Swarm::new_ephemeral(|_| ReplicationBehaviour::new());
        let mut swarm2 = Swarm::new_ephemeral(|_| ReplicationBehaviour::new());

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
        let mut swarm1 = Swarm::new_ephemeral(|_| ReplicationBehaviour::new());
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
        let mut swarm1 = Swarm::new_ephemeral(|_| ReplicationBehaviour::new());
        let mut swarm2 = Swarm::new_ephemeral(|_| ReplicationBehaviour::new());

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
