// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use asynchronous_codec::Framed;
use futures::{Sink, StreamExt};
use libp2p::swarm::handler::{
    ConnectionEvent, FullyNegotiatedInbound, FullyNegotiatedOutbound, ProtocolsChange,
};
use libp2p::swarm::{
    ConnectionHandler, ConnectionHandlerEvent, KeepAlive, Stream as NegotiatedStream,
    SubstreamProtocol,
};
use log::{debug, warn};
use thiserror::Error;

use crate::network::peers::{Codec, CodecError, Protocol};
use crate::replication::SyncMessage;

/// The time a connection is maintained to a peer without being in live mode and without
/// send/receiving a message from. Connections that idle beyond this timeout are disconnected.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Handler for an incoming or outgoing connection to a remote peer dealing with the p2panda
/// protocol.
///
/// Manages the bi-directional data streams and encodes and decodes p2panda messages on them using
/// the CBOR format.
///
/// Connection handlers can be closed due to critical errors, for example when a replication error
/// occurred. They also can close after a certain duration of no networking activity (timeout).
/// Note that this does _not_ close the connection to the peer in general, only the p2panda
/// messaging protocol.
///
/// Usually one connection is established to one peer. Multiple connections to the same peer are
/// also possible. This especially is the case when both peers dial each other at the same time.
///
/// Each connection is managed by one connection handler each. Inside of each connection we
/// maintain a bi-directional (inbound & outbound) data stream.
///
/// The following diagram is an example of two connections from one local to one remote peer:
///
/// ```text
///       Connection
///       (Incoming)
/// ┌───────────────────┐
/// │                   │
/// │  ┌─────────────┐  │          ┌─────────────┐
/// │  │   Stream    ◄──┼──────────┤             │
/// │  │  (Inbound)  │  │          │             │
/// │  └─────────────┘  │          │             │
/// │                   │          │             │
/// │  ┌─────────────┐  │          │             │
/// │  │   Stream    ├──┼──────────►             │
/// │  │  (Outbound) │  │          │             │
/// │  └─────────────┘  │          │             │
/// │                   │          │             │
/// └───────────────────┘          │             │
///                                │             │
///       Connection               │ Remote Peer │
///       (Outgoing)               │             │
/// ┌───────────────────┐          │             │
/// │                   │          │             │
/// │  ┌─────────────┐  │          │             │
/// │  │   Stream    ◄──┼──────────┤             │
/// │  │  (Inbound)  │  │          │             │
/// │  └─────────────┘  │          │             │
/// │                   │          │             │
/// │  ┌─────────────┐  │          │             │
/// │  │   Stream    ├──┼──────────►             │
/// │  │  (Outbound) │  │          │             │
/// │  └─────────────┘  │          └─────────────┘
/// │                   │
/// └───────────────────┘
/// ```
pub struct Handler {
    /// Upgrade configuration for the protocol.
    listen_protocol: SubstreamProtocol<Protocol, ()>,

    /// The single long-lived outbound substream.
    outbound_substream: Option<OutboundSubstreamState>,

    /// The single long-lived inbound substream.
    inbound_substream: Option<InboundSubstreamState>,

    /// Flag indicating that an outbound substream is being established to prevent duplicate
    /// requests.
    outbound_substream_establishing: bool,

    /// Queue of messages that we want to send to the remote.
    send_queue: VecDeque<SyncMessage>,

    /// Last time we've observed inbound or outbound messaging activity.
    last_io_activity: Instant,

    /// Flag indicating that we want to close connection handlers related to that peer.
    ///
    /// This is useful in scenarios where a critical error occurred outside of the libp2p stack
    /// (for example in the replication service) and we need to accordingly close connections.
    critical_error: bool,
}

impl Handler {
    pub fn new() -> Self {
        Self {
            listen_protocol: SubstreamProtocol::new(Protocol::new(), ()),
            outbound_substream: None,
            inbound_substream: None,
            outbound_substream_establishing: false,
            send_queue: VecDeque::new(),
            last_io_activity: Instant::now(),
            critical_error: false,
        }
    }

    fn on_fully_negotiated_outbound(
        &mut self,
        FullyNegotiatedOutbound { protocol, info: () }: FullyNegotiatedOutbound<
            <Self as ConnectionHandler>::OutboundProtocol,
            <Self as ConnectionHandler>::OutboundOpenInfo,
        >,
    ) {
        self.outbound_substream = Some(OutboundSubstreamState::WaitingOutput(protocol));
        self.outbound_substream_establishing = false;
    }

    fn on_fully_negotiated_inbound(
        &mut self,
        FullyNegotiatedInbound { protocol, .. }: FullyNegotiatedInbound<
            <Self as ConnectionHandler>::InboundProtocol,
            <Self as ConnectionHandler>::InboundOpenInfo,
        >,
    ) {
        self.inbound_substream = Some(InboundSubstreamState::WaitingInput(protocol));
    }
}

/// An event sent from the network behaviour to the connection handler.
#[derive(Debug)]
pub enum HandlerFromBehaviour {
    /// Message to send on outbound stream.
    Message(SyncMessage),

    /// Protocol failed with a critical error.
    CriticalError,
}

/// The event emitted by the connection handler.
///
/// This informs the network behaviour of various events created by the handler.
#[derive(Debug)]
pub enum HandlerToBehaviour {
    /// Message received on the inbound stream.
    Message(SyncMessage),
}

#[derive(Debug, Error)]
pub enum HandlerError {
    #[error("Failed to encode or decode CBOR")]
    Codec(#[from] CodecError),
}

type Stream = Framed<NegotiatedStream, Codec>;

/// State of the inbound substream, opened either by us or by the remote.
enum InboundSubstreamState {
    /// Waiting for a message from the remote. The idle state for an inbound substream.
    WaitingInput(Stream),

    /// The substream is being closed.
    Closing(Stream),

    /// An error occurred during processing.
    Poisoned,
}

/// State of the outbound substream, opened either by us or by the remote.
enum OutboundSubstreamState {
    /// Waiting for the user to send a message. The idle state for an outbound substream.
    WaitingOutput(Stream),

    /// Waiting to send a message to the remote.
    PendingSend(Stream, SyncMessage),

    /// Waiting to flush the substream so that the data arrives to the remote.
    PendingFlush(Stream),

    /// An error occurred during processing.
    Poisoned,
}

impl ConnectionHandler for Handler {
    type FromBehaviour = HandlerFromBehaviour;
    type ToBehaviour = HandlerToBehaviour;
    type Error = HandlerError;
    type InboundProtocol = Protocol;
    type OutboundProtocol = Protocol;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        self.listen_protocol.clone()
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
            ConnectionEvent::FullyNegotiatedOutbound(fully_negotiated_outbound) => {
                self.on_fully_negotiated_outbound(fully_negotiated_outbound);
            }
            ConnectionEvent::FullyNegotiatedInbound(fully_negotiated_inbound) => {
                self.on_fully_negotiated_inbound(fully_negotiated_inbound);
            }
            ConnectionEvent::DialUpgradeError(_)
            | ConnectionEvent::AddressChange(_)
            | ConnectionEvent::ListenUpgradeError(_) => {
                warn!("Connection event error");
            }
            ConnectionEvent::LocalProtocolsChange(_) => {
                debug!("ConnectionEvent: LocalProtocolsChange")
            }
            ConnectionEvent::RemoteProtocolsChange(_) => {
                debug!("ConnectionEvent: RemoteProtocolsChange")
            }
        }
    }

    fn on_behaviour_event(&mut self, event: Self::FromBehaviour) {
        match event {
            HandlerFromBehaviour::Message(message) => {
                self.send_queue.push_back(message);
            }
            HandlerFromBehaviour::CriticalError => {
                self.critical_error = true;
            }
        }
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        if self.critical_error {
            return KeepAlive::No;
        }

        if let Some(
            OutboundSubstreamState::PendingSend(_, _) | OutboundSubstreamState::PendingFlush(_),
        ) = self.outbound_substream
        {
            return KeepAlive::Yes;
        }

        KeepAlive::Until(self.last_io_activity + IDLE_TIMEOUT)
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<
            Self::OutboundProtocol,
            Self::OutboundOpenInfo,
            Self::ToBehaviour,
            Self::Error,
        >,
    > {
        // Determine if we need to create the outbound stream
        if !self.send_queue.is_empty()
            && self.outbound_substream.is_none()
            && !self.outbound_substream_establishing
        {
            self.outbound_substream_establishing = true;
            return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                protocol: self.listen_protocol(),
            });
        }

        // Process inbound stream
        loop {
            match std::mem::replace(
                &mut self.inbound_substream,
                Some(InboundSubstreamState::Poisoned),
            ) {
                Some(InboundSubstreamState::WaitingInput(mut substream)) => {
                    match substream.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(message))) => {
                            self.last_io_activity = Instant::now();

                            // Received message from remote peer
                            self.inbound_substream =
                                Some(InboundSubstreamState::WaitingInput(substream));

                            return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(
                                HandlerToBehaviour::Message(message),
                            ));
                        }
                        Poll::Ready(Some(Err(err))) => {
                            warn!("Error decoding inbound message: {err}");

                            // Close this side of the stream. If the peer is still around, they
                            // will re-establish their connection
                            self.inbound_substream =
                                Some(InboundSubstreamState::Closing(substream));
                        }
                        Poll::Ready(None) => {
                            // Remote peer closed the stream
                            self.inbound_substream =
                                Some(InboundSubstreamState::Closing(substream));
                        }
                        Poll::Pending => {
                            self.inbound_substream =
                                Some(InboundSubstreamState::WaitingInput(substream));
                            break;
                        }
                    }
                }
                Some(InboundSubstreamState::Closing(mut substream)) => {
                    match Sink::poll_close(Pin::new(&mut substream), cx) {
                        Poll::Ready(res) => {
                            if let Err(err) = res {
                                // Don't close the connection but just drop the inbound substream.
                                // In case the remote has more to send, they will open up a new
                                // substream.
                                warn!("Error during closing inbound connection: {err}")
                            }
                            self.inbound_substream = None;
                            break;
                        }
                        Poll::Pending => {
                            self.inbound_substream =
                                Some(InboundSubstreamState::Closing(substream));
                            break;
                        }
                    }
                }
                None => {
                    self.inbound_substream = None;
                    break;
                }
                Some(InboundSubstreamState::Poisoned) => {
                    unreachable!("Error occurred during inbound stream processing")
                }
            }
        }

        // Process outbound stream
        loop {
            match std::mem::replace(
                &mut self.outbound_substream,
                Some(OutboundSubstreamState::Poisoned),
            ) {
                Some(OutboundSubstreamState::WaitingOutput(substream)) => {
                    match self.send_queue.pop_front() {
                        Some(message) => {
                            self.outbound_substream =
                                Some(OutboundSubstreamState::PendingSend(substream, message));

                            // Continue loop in case there is more messages to be sent
                            continue;
                        }
                        None => {
                            self.outbound_substream =
                                Some(OutboundSubstreamState::WaitingOutput(substream));
                            break;
                        }
                    }
                }
                Some(OutboundSubstreamState::PendingSend(mut substream, message)) => {
                    match Sink::poll_ready(Pin::new(&mut substream), cx) {
                        Poll::Ready(Ok(())) => {
                            match Sink::start_send(Pin::new(&mut substream), message) {
                                Ok(()) => {
                                    self.outbound_substream =
                                        Some(OutboundSubstreamState::PendingFlush(substream))
                                }
                                Err(err) => {
                                    warn!("Error sending outbound message: {err}");
                                    self.outbound_substream = None;
                                    break;
                                }
                            }
                        }
                        Poll::Ready(Err(err)) => {
                            warn!("Error encoding outbound message: {err}");
                            self.outbound_substream = None;
                            break;
                        }
                        Poll::Pending => {
                            self.outbound_substream =
                                Some(OutboundSubstreamState::PendingSend(substream, message));
                            break;
                        }
                    }
                }
                Some(OutboundSubstreamState::PendingFlush(mut substream)) => {
                    match Sink::poll_flush(Pin::new(&mut substream), cx) {
                        Poll::Ready(Ok(())) => {
                            self.last_io_activity = Instant::now();
                            self.outbound_substream =
                                Some(OutboundSubstreamState::WaitingOutput(substream))
                        }
                        Poll::Ready(Err(err)) => {
                            warn!("Error flushing outbound message: {err}");
                            self.outbound_substream = None;
                            break;
                        }
                        Poll::Pending => {
                            self.outbound_substream =
                                Some(OutboundSubstreamState::PendingFlush(substream));
                            break;
                        }
                    }
                }
                None => {
                    self.outbound_substream = None;
                    break;
                }
                Some(OutboundSubstreamState::Poisoned) => {
                    unreachable!("Error occurred during outbound stream processing")
                }
            }
        }

        Poll::Pending
    }
}
