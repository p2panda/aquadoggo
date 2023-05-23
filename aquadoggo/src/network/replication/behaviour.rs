// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::VecDeque;
use std::task::{Context, Poll};

use libp2p::core::Endpoint;
use libp2p::swarm::derive_prelude::ConnectionEstablished;
use libp2p::swarm::{
    ConnectionDenied, ConnectionId, FromSwarm, NetworkBehaviour, NotifyHandler, PollParameters,
    THandler, THandlerInEvent, THandlerOutEvent, ToSwarm,
};
use libp2p::{Multiaddr, PeerId};

use crate::db::SqlStore;
use crate::network::replication::handler::{Handler, HandlerInEvent, HandlerOutEvent};
use crate::replication::{Message, SyncIngest, SyncManager, SyncMessage, TargetSet};
use crate::schema::SchemaProvider;

#[derive(Debug)]
pub enum BehaviourOutEvent {
    MessageReceived(PeerId, SyncMessage),
    Error,
}

#[derive(Debug)]
pub struct Behaviour {
    events: VecDeque<ToSwarm<BehaviourOutEvent, HandlerInEvent>>,
    manager: SyncManager<PeerId>,
    schema_provider: SchemaProvider,
}

impl Behaviour {
    pub fn new(
        store: &SqlStore,
        ingest: SyncIngest,
        schema_provider: SchemaProvider,
        peer_id: &PeerId,
    ) -> Self {
        Self {
            events: VecDeque::new(),
            manager: SyncManager::new(store.clone(), ingest, peer_id.clone()),
            schema_provider,
        }
    }
}

impl Behaviour {
    fn send_message(&mut self, peer_id: PeerId, message: SyncMessage) {
        self.events.push_back(ToSwarm::NotifyHandler {
            peer_id,
            event: HandlerInEvent::Message(message),
            handler: NotifyHandler::Any,
        });
    }

    fn handle_received_message(&mut self, peer_id: &PeerId, message: SyncMessage) {
        // @TODO: Handle incoming messages
        self.events
            .push_back(ToSwarm::GenerateEvent(BehaviourOutEvent::MessageReceived(
                *peer_id, message,
            )));
    }

    fn handle_established_connection(&mut self, remote_peer_id: &PeerId) {
        // @TODO: Have an async backend
        self.send_message(
            *remote_peer_id,
            SyncMessage::new(0, Message::SyncRequest(0.into(), TargetSet::new(&vec![]))),
        )
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
            FromSwarm::ConnectionEstablished(ConnectionEstablished { peer_id, .. }) => {
                self.handle_established_connection(&peer_id)
            }
            FromSwarm::ConnectionClosed(_)
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

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use libp2p::swarm::{keep_alive, Swarm};
    use libp2p::PeerId;
    use libp2p_swarm_test::SwarmExt;
    use tokio::sync::broadcast;

    use crate::replication::{Message, SyncIngest, SyncMessage, TargetSet};
    use crate::test_utils::{test_runner, test_runner_with_manager, TestNode, TestNodeManager};

    use super::{Behaviour as ReplicationBehaviour, BehaviourOutEvent};

    #[tokio::test]
    async fn peers_connect() {
        let (tx, _rx) = broadcast::channel(8);

        test_runner_with_manager(|manager: TestNodeManager| async move {
            let node_a = manager.create().await;
            let node_b = manager.create().await;

            let peer_id_a = PeerId::random();
            let peer_id_b = PeerId::random();

            // Create two swarms
            let mut swarm1 = Swarm::new_ephemeral(|_| {
                ReplicationBehaviour::new(
                    &node_a.context.store,
                    SyncIngest::new(node_a.context.schema_provider.clone(), tx.clone()),
                    node_a.context.schema_provider.clone(),
                    &peer_id_a,
                )
            });
            let mut swarm2 = Swarm::new_ephemeral(|_| {
                ReplicationBehaviour::new(
                    &node_a.context.store,
                    SyncIngest::new(node_b.context.schema_provider.clone(), tx.clone()),
                    node_b.context.schema_provider.clone(),
                    &peer_id_b,
                )
            });

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
        });
    }

    #[tokio::test]
    async fn incompatible_network_behaviour() {
        test_runner(|node: TestNode| async move {
            let (tx, _rx) = broadcast::channel(8);
            let peer_id = PeerId::random();

            // Create two swarms
            let mut swarm1 = Swarm::new_ephemeral(|_| {
                ReplicationBehaviour::new(
                    &node.context.store,
                    SyncIngest::new(node.context.schema_provider.clone(), tx.clone()),
                    node.context.schema_provider.clone(),
                    &peer_id,
                )
            });

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
        });
    }

    #[tokio::test]
    async fn swarm_behaviour_events() {
        let (tx, _rx) = broadcast::channel(8);

        test_runner_with_manager(|manager: TestNodeManager| async move {
            let node_a = manager.create().await;
            let node_b = manager.create().await;

            let peer_id_a = PeerId::random();
            let peer_id_b = PeerId::random();

            // Create two swarms
            let mut swarm1 = Swarm::new_ephemeral(|_| {
                ReplicationBehaviour::new(
                    &node_a.context.store,
                    SyncIngest::new(node_a.context.schema_provider.clone(), tx.clone()),
                    node_a.context.schema_provider.clone(),
                    &peer_id_a,
                )
            });
            let mut swarm2 = Swarm::new_ephemeral(|_| {
                ReplicationBehaviour::new(
                    &node_a.context.store,
                    SyncIngest::new(node_b.context.schema_provider.clone(), tx.clone()),
                    node_b.context.schema_provider.clone(),
                    &peer_id_b,
                )
            });

            // Listen on swarm1 and connect from swarm2, this should establish a bi-directional connection.
            swarm1.listen().await;
            swarm2.connect(&mut swarm1).await;

            let mut res1 = Vec::new();
            let mut res2 = Vec::new();

            let swarm1_peer_id = *swarm1.local_peer_id();
            let swarm2_peer_id = *swarm2.local_peer_id();

            // Send a message from to swarm1 local peer from swarm2 local peer.
            swarm1.behaviour_mut().send_message(
                swarm2_peer_id,
                SyncMessage::new(0, Message::SyncRequest(0.into(), TargetSet::new(&vec![]))),
            );

            // Send a message from to swarm2 local peer from swarm1 local peer.
            swarm2.behaviour_mut().send_message(
                swarm1_peer_id,
                SyncMessage::new(0, Message::SyncRequest(0.into(), TargetSet::new(&vec![]))),
            );

            // Collect the next 2 behaviour events which occur in either swarms.
            for _ in 0..2 {
                tokio::select! {
                    BehaviourOutEvent::MessageReceived(peer_id, message) = swarm1.next_behaviour_event() => res1.push((peer_id, message)),
                    BehaviourOutEvent::MessageReceived(peer_id, message) = swarm2.next_behaviour_event() => res2.push((peer_id, message)),
                }
            }

            // Each swarm should have emitted exactly one event.
            assert_eq!(res1.len(), 1);
            assert_eq!(res2.len(), 1);

            // swarm1 should have received the message from swarm2 peer.
            let (peer_id, message) = &res1[0];
            assert_eq!(peer_id, &swarm2_peer_id);
            assert_eq!(
                message,
                &SyncMessage::new(0, Message::SyncRequest(0.into(), TargetSet::new(&vec![])))
            );

            // swarm2 should have received the message from swarm1 peer.
            let (peer_id, message) = &res2[0];
            assert_eq!(peer_id, &swarm1_peer_id);
            assert_eq!(
                message,
                &SyncMessage::new(0, Message::SyncRequest(0.into(), TargetSet::new(&vec![])))
            );
        });
    }
}
