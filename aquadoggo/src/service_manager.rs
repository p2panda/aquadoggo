// SPDX-License-Identifier: AGPL-3.0-or-later

//! Service manager for orchestration of long-running concurrent processes.
//!
//! This manager offers a message bus between services for cross-service communication. It also
//! sends a shutdown signal to allow services to react to it gracefully.
use std::future::Future;
use std::sync::Arc;

use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::task;
use tokio::task::JoinHandle;

/// Sends messages through the communication bus between services.
pub type Sender<T> = broadcast::Sender<T>;

/// Receives shutdown signal for services so they can react accordingly.
pub type Shutdown = JoinHandle<()>;

/// Data shared across services, for example a database.
pub struct Context<D: Send + Sync + 'static>(Arc<D>);

impl<D: Send + Sync + 'static> Clone for Context<D> {
    /// This `clone` implementation efficiently increments the reference counter to the inner
    /// object instead of actually cloning it.
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

/// This trait defines a generic async service function receiving a shared context and access to
/// the communication bus and shutdown signal handler.
///
/// It is also using the `async_trait` macro as a trick to avoid a more ugly trait signature as
/// working with generic, static, pinned and boxed async functions can look quite messy.
#[async_trait::async_trait]
pub trait Service<D, M>
where
    D: Send + Sync + 'static,
    M: Clone + Send + Sync + 'static,
{
    async fn call(&self, context: Context<D>, shutdown: Shutdown, tx: Sender<M>);
}

/// Implements our `Service` trait for a generic async function.
#[async_trait::async_trait]
impl<FN, F, D, M> Service<D, M> for FN
where
    // Function accepting a context and our communication channels, returning a future.
    FN: Fn(Context<D>, Shutdown, Sender<M>) -> F + Sync,
    // A future
    F: Future<Output = ()> + Send + 'static,
    // Generic context type.
    D: Send + Sync + 'static,
    // Generic message type for the communication bus.
    M: Clone + Send + Sync + 'static,
{
    /// Internal method which calls our generic async function, passing in the context and channels
    /// for communication.
    ///
    /// This gets automatically wrapped in a static, boxed and pinned function signature by the
    /// `async_trait` macro so we don't need to do it ourselves.
    async fn call(&self, context: Context<D>, shutdown: Shutdown, tx: Sender<M>) {
        (self)(context, shutdown, tx).await
    }
}

pub struct ServiceManager<D, M>
where
    D: Send + Sync + 'static,
    M: Clone + Send + Sync + 'static,
{
    /// Shared, thread-safe context between services.
    context: Context<D>,

    /// Sender of our communication bus.
    ///
    /// This is a broadcast channel where any amount of senders and receivers can be derived from.
    tx: Sender<M>,

    /// Sender of the shutdown signal.
    ///
    /// All services can subscribe to this broadcast channel and accordingly react to it if they
    /// need to.
    shutdown: broadcast::Sender<bool>,
}

impl<D, M> ServiceManager<D, M>
where
    D: Send + Sync + 'static,
    M: Clone + Send + Sync + 'static,
{
    /// Returns a new instance of a service manager.
    ///
    /// The capacity argument defines the maximum bound of messages on the communication bus which
    /// get broadcasted across all services.
    pub fn new(capacity: usize, context: D) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        let (shutdown, _) = broadcast::channel(16);

        Self {
            context: Context(Arc::new(context)),
            tx,
            shutdown,
        }
    }

    /// Adds a new service to the manager.
    pub fn add<F: Service<D, M> + Send + Sync + Copy + 'static>(&mut self, service: F) {
        // Sender for communication bus
        let tx = self.tx.clone();

        // Sender and receiver for shutdown channel
        let shutdown_tx = self.shutdown.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();

        // Wait for any signal from the shutdown channel
        let signal = task::spawn(async move {
            let _ = shutdown_rx.recv().await;
        });

        // Reference to shared context
        let context = self.context.clone();

        task::spawn(async move {
            // Run the service!
            service.call(context, signal, tx).await;

            // Drop the shutdown sender of this service when we're done, this signals the shutdown
            // process that this service has finally stopped
            drop(shutdown_tx);
        });
    }

    /// Informs all services about graceful shutdown and waits for them until they all stopped.
    pub async fn shutdown(self) {
        let mut rx = self.shutdown.subscribe();

        // Broadcast graceful shutdown messages to all services
        self.shutdown.send(true).unwrap();

        // We drop our sender first to make sure _all_ senders get eventually closed, because the
        // recv() call otherwise sleeps forever.
        drop(self.shutdown);

        // When every sender has gone out of scope, the recv call will return with a `Closed`
        // error. This is our signal that all services have been finally shut down and we are done
        // for good!
        loop {
            match rx.recv().await {
                Err(RecvError::Closed) => break,
                _ => (),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::{Context, Sender, ServiceManager, Shutdown};

    #[tokio::test]
    async fn service_manager() {
        let mut manager = ServiceManager::<usize, usize>::new(16, 0);

        manager.add(|_, signal: Shutdown, _| async move {
            let work = tokio::task::spawn(async {
                loop {
                    // Doing some very important work here ..
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            });

            // Stop when we received shutdown signal or when work was done
            tokio::select! { _ = work => (), _ = signal => () };

            // Some "tidying" we have to do before we can actually close this service
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        });

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn communication_bus() {
        // Messages we can send through the communication bus
        #[derive(Clone, Debug)]
        enum Message {
            Hello,
        }

        // Counter which is shared between services
        type Counter = Arc<AtomicUsize>;
        let counter: Counter = Arc::new(AtomicUsize::new(0));

        let mut manager = ServiceManager::<Counter, Message>::new(32, counter.clone());

        // Create five services waiting for message
        for _ in 0..5 {
            manager.add(
                |data: Context<Counter>, _, tx: Sender<Message>| async move {
                    let mut rx = tx.subscribe();
                    let message = rx.recv().await.unwrap();

                    // Increment counter as soon as we received the right message
                    if matches!(message, Message::Hello) {
                        data.0.fetch_add(1, Ordering::Relaxed);
                    }
                },
            );
        }

        // Create another service sending message over communication bus
        manager.add(|_, _, tx: Sender<Message>| async move {
            tx.send(Message::Hello).unwrap();
        });

        manager.shutdown().await;

        // Check if we received the message in all services
        assert_eq!(counter.load(Ordering::Relaxed), 5);
    }
}
