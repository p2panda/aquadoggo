// SPDX-License-Identifier: AGPL-3.0-or-later

use std::pin::Pin;
use std::task::{Context, Poll};

use asynchronous_codec::Framed;
use deadqueue::limited::Queue;
use futures::{Sink, StreamExt};
use libp2p::swarm::handler::{
    ConnectionEvent, DialUpgradeError, FullyNegotiatedInbound, FullyNegotiatedOutbound,
};
use libp2p::swarm::{
    ConnectionHandler, ConnectionHandlerEvent, KeepAlive, NegotiatedSubstream, SubstreamProtocol,
};
use thiserror::Error;

use crate::network::replication::{Codec, CodecError, Message, Protocol};

pub struct Handler {
    /// Upgrade configuration for the replication protocol.
    listen_protocol: SubstreamProtocol<Protocol, ()>,

    /// The single long-lived outbound substream.
    outbound_substream: Option<OutboundSubstreamState>,

    /// The single long-lived inbound substream.
    inbound_substream: Option<InboundSubstreamState>,

    /// Flag indicating that an outbound substream is being established to prevent duplicate
    /// requests.
    outbound_substream_establishing: bool,

    /// Queue of messages that we want to send to the remote.
    send_queue: Queue<Message>,
}

impl Handler {
    pub fn new() -> Self {
        Self {
            listen_protocol: SubstreamProtocol::new(Protocol::new(), ()),
            outbound_substream: None,
            inbound_substream: None,
            outbound_substream_establishing: false,
            send_queue: Queue::new(16),
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

    fn on_dial_upgrade_error(
        &mut self,
        DialUpgradeError {
            info: (),
            error: _error,
            ..
        }: DialUpgradeError<
            <Self as ConnectionHandler>::OutboundOpenInfo,
            <Self as ConnectionHandler>::OutboundProtocol,
        >,
    ) {
        todo!()
    }
}

/// An event sent from the network behaviour to the connection handler.
#[derive(Debug)]
pub enum InEvent {
    /// Replication message to send on outbound stream.
    Message(Message),
}

/// The event emitted by the connection handler.
///
/// This informs the network behaviour of various events created by the handler.
#[derive(Debug)]
pub enum OutEvent {
    /// Replication message received on the inbound stream.
    Message(Message),
}

// @TODO: Do we need our own error type? Use `Void` instead?
#[derive(Debug, Error)]
pub enum HandlerError {
    #[error("Failed to encode or decode CBOR")]
    Codec(#[from] CodecError),
}

type Stream = Framed<NegotiatedSubstream, Codec>;

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
    PendingSend(Stream, Message),

    /// Waiting to flush the substream so that the data arrives to the remote.
    PendingFlush(Stream),

    /// An error occurred during processing.
    Poisoned,
}

impl ConnectionHandler for Handler {
    type InEvent = InEvent;
    type OutEvent = OutEvent;
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
            ConnectionEvent::DialUpgradeError(dial_upgrade_error) => {
                self.on_dial_upgrade_error(dial_upgrade_error)
            }
            ConnectionEvent::AddressChange(_) | ConnectionEvent::ListenUpgradeError(_) => {}
        }
    }

    fn on_behaviour_event(&mut self, _event: Self::InEvent) {
        todo!()
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        // @TODO
        KeepAlive::No
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<
            Self::OutboundProtocol,
            Self::OutboundOpenInfo,
            Self::OutEvent,
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
                            // Received message from remote peer
                            self.inbound_substream =
                                Some(InboundSubstreamState::WaitingInput(substream));
                            return Poll::Ready(ConnectionHandlerEvent::Custom(OutEvent::Message(
                                message,
                            )));
                        }
                        Poll::Ready(Some(Err(_))) => {
                            // More serious errors, close this side of the stream. If the peer is
                            // still around, they will re-establish their connection
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
                            if res.is_err() {
                                // Don't close the connection but just drop the inbound substream.
                                // In case the remote has more to send, they will open up a new
                                // substream.
                                // @TODO: Log error here
                            }

                            self.inbound_substream = None;

                            // @TODO: Handle keep alive
                            // if self.outbound_substream.is_none() {
                            //     self.keep_alive = KeepAlive::No;
                            // }

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
                    match self.send_queue.try_pop() {
                        Some(message) => {
                            self.outbound_substream =
                                Some(OutboundSubstreamState::PendingSend(substream, message));
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
                                    return Poll::Ready(ConnectionHandlerEvent::Close(
                                        HandlerError::Codec(err),
                                    ));
                                }
                            }
                        }
                        Poll::Ready(Err(err)) => {
                            return Poll::Ready(ConnectionHandlerEvent::Close(
                                HandlerError::Codec(err),
                            ));
                        }
                        Poll::Pending => {
                            // @TODO: Handle keep alive
                            // self.keep_alive = KeepAlive::Yes;

                            self.outbound_substream =
                                Some(OutboundSubstreamState::PendingSend(substream, message));
                            break;
                        }
                    }
                }
                Some(OutboundSubstreamState::PendingFlush(mut substream)) => {
                    match Sink::poll_flush(Pin::new(&mut substream), cx) {
                        Poll::Ready(Ok(())) => {
                            self.outbound_substream =
                                Some(OutboundSubstreamState::WaitingOutput(substream))
                        }
                        Poll::Ready(Err(err)) => {
                            return Poll::Ready(ConnectionHandlerEvent::Close(HandlerError::Codec(
                                err,
                            )))
                        }
                        Poll::Pending => {
                            // @TODO: Handle keep alive
                            // self.keep_alive = KeepAlive::Yes;

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
