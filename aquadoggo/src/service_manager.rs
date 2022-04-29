// SPDX-License-Identifier: AGPL-3.0-or-later

use std::future::Future;

use anyhow::Result;
use log::{error, info};
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::task;
use tokio::task::JoinHandle;
use triggered::{Listener, Trigger};

/// Sends messages through the communication bus between services.
pub type Sender<T> = broadcast::Sender<T>;

/// Receives shutdown signal for services so they can react accordingly.
pub type Shutdown = JoinHandle<()>;

/// This trait defines a generic async service function receiving a shared context and access to
/// the communication bus and shutdown signal handler.
///
/// It is also using the `async_trait` macro as a trick to avoid a more ugly trait signature as
/// working with generic, static, pinned and boxed async functions can look quite messy.
#[async_trait::async_trait]
pub trait Service<D, M>
where
    D: Clone + Send + Sync + 'static,
    M: Clone + Send + Sync + 'static,
{
    async fn call(&self, context: D, shutdown: Shutdown, tx: Sender<M>) -> Result<()>;
}

/// Implements our `Service` trait for a generic async function.
#[async_trait::async_trait]
impl<FN, F, D, M> Service<D, M> for FN
where
    // Function accepting a context and our communication channels, returning a future.
    FN: Fn(D, Shutdown, Sender<M>) -> F + Sync,
    // A future
    F: Future<Output = Result<()>> + Send + 'static,
    // Generic context type.
    D: Clone + Send + Sync + 'static,
    // Generic message type for the communication bus.
    M: Clone + Send + Sync + 'static,
{
    /// Internal method which calls our generic async function, passing in the context and channels
    /// for communication.
    ///
    /// This gets automatically wrapped in a static, boxed and pinned function signature by the
    /// `async_trait` macro so we don't need to do it ourselves.
    async fn call(&self, context: D, shutdown: Shutdown, tx: Sender<M>) -> Result<()> {
        (self)(context, shutdown, tx).await
    }
}

/// Wrapper around `Trigger` which sends a signal as soon as `Signal` gets dropped.
#[derive(Clone)]
struct Signal(Trigger);

impl Signal {
    /// Fires the signal manually.
    pub fn trigger(&self) {
        self.0.trigger();
    }
}

impl Drop for Signal {
    fn drop(&mut self) {
        // Fires the signal automatically on drop
        self.trigger();

        // And now, drop it!
        drop(self);
    }
}

// Service manager for orchestration of long-running concurrent processes.
//
// This manager offers a message bus between services for cross-service communication. It also
// sends a shutdown signal to allow services to react to it gracefully.
//
// Stopped services (because of a panic, error or successful return) will send an exit signal which
// can be subscribed to via the `on_exit` method. Usually stopped services indicate system failure
// and it is recommended to stop the application when this events occurs.
pub struct ServiceManager<D, M>
where
    D: Clone + Send + Sync + 'static,
    M: Clone + Send + Sync + 'static,
{
    /// Shared, thread-safe context between services.
    context: D,

    /// Sender of our communication bus.
    tx: Sender<M>,

    /// Sender of exit signal.
    ///
    /// The manager catches returned errors or panics from services and sends the exit signal.
    exit_signal: Signal,

    /// Receiver of exit signal.
    ///
    /// This can be used to react to service errors, for example by quitting the program.
    exit_handle: Listener,

    /// Sender of shutdown signal.
    ///
    /// All services can subscribe to this broadcast channel and accordingly react to it.
    ///
    /// This needs to be a broadcast channel as we keep count of the subscribers and stop the
    /// service manager as soon as all of them have been dropped.
    shutdown_signal: broadcast::Sender<bool>,
}

impl<D, M> ServiceManager<D, M>
where
    D: Clone + Send + Sync + 'static,
    M: Clone + Send + Sync + 'static,
{
    /// Returns a new instance of a service manager.
    ///
    /// The `capacity` argument defines the maximum bound of messages on the communication bus
    /// which get broadcasted across all services.
    pub fn new(capacity: usize, context: D) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        let (shutdown_signal, _) = broadcast::channel(16);
        let (exit_signal, exit_handle) = triggered::trigger();

        Self {
            context,
            tx,
            exit_signal: Signal(exit_signal),
            exit_handle,
            shutdown_signal,
        }
    }

    /// Adds a new service to the manager.
    ///
    /// Errors returned and panics by the service will send an exit signal which can be subscribed
    /// to via the `on_exit` method.
    pub fn add<F: Service<D, M> + Send + Sync + Copy + 'static>(
        &mut self,
        name: &'static str,
        service: F,
    ) {
        // Sender for communication bus
        let tx = self.tx.clone();

        // Sender and receiver for shutdown channel
        let shutdown_tx = self.shutdown_signal.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();

        // Wait for any signal from the shutdown channel
        let signal = task::spawn(async move {
            let _ = shutdown_rx.recv().await;
        });

        // Sender for exit signal
        let exit_signal = self.exit_signal.clone();

        // Reference to shared context
        let context = self.context.clone();

        task::spawn(async move {
            info!("Start {} service", name);

            // Run the service!
            let handle = service.call(context, signal, tx).await;

            // Drop the shutdown sender of this service when we're done, this signals the shutdown
            // process that this service has finally stopped
            drop(shutdown_tx);

            // Handle potential errors which have been returned by the service.
            if let Some(err) = handle.err() {
                error!("Error in {} service: {}", name, err);
                exit_signal.trigger();
            }

            // `exit_signal` will go out of scope now and drops here. Since we also implemented the
            // `Drop` trait on `Signal` we will be able to fire a signal also when this task panics
            // or stops.
        });
    }

    /// Future which resolves as soon as a service returned an error, panicked or stopped.
    pub async fn on_exit(&self) {
        self.exit_handle.clone().await;
    }

    /// Informs all services about graceful shutdown and waits for them until they all stopped.
    pub async fn shutdown(self) {
        info!("Received shutdown signal");

        let mut rx = self.shutdown_signal.subscribe();

        // Broadcast graceful shutdown messages to all services
        self.shutdown_signal.send(true).unwrap();

        // We drop our sender first to make sure _all_ senders get eventually closed, because the
        // recv() call otherwise sleeps forever.
        drop(self.shutdown_signal);

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

    use super::{Sender, ServiceManager, Shutdown};

    type Counter = Arc<AtomicUsize>;

    #[tokio::test]
    async fn service_manager() {
        let mut manager = ServiceManager::<usize, usize>::new(16, 0);

        manager.add("test", |_, signal: Shutdown, _| async move {
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

            Ok(())
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
        let counter: Counter = Arc::new(AtomicUsize::new(0));

        let mut manager = ServiceManager::<Counter, Message>::new(32, counter.clone());

        // Create five services waiting for message
        for _ in 0..5 {
            manager.add("rx", |data: Counter, _, tx: Sender<Message>| async move {
                let mut rx = tx.subscribe();
                let message = rx.recv().await.unwrap();

                // Increment counter as soon as we received the right message
                if matches!(message, Message::Hello) {
                    data.fetch_add(1, Ordering::Relaxed);
                }

                Ok(())
            });
        }

        // Create another service sending message over communication bus
        manager.add("tx", |_, _, tx: Sender<Message>| async move {
            tx.send(Message::Hello).unwrap();
            Ok(())
        });

        manager.shutdown().await;

        // Check if we received the message in all services
        assert_eq!(counter.load(Ordering::Relaxed), 5);
    }

    #[tokio::test]
    async fn on_exit() {
        let counter: Counter = Arc::new(AtomicUsize::new(0));
        let mut manager = ServiceManager::<Counter, usize>::new(32, counter.clone());

        manager.add("one", |counter: Counter, signal: Shutdown, _| async move {
            let counter_clone = counter.clone();

            let work = tokio::task::spawn(async move {
                // Increment counter once within the work task
                counter_clone.fetch_add(1, Ordering::Relaxed);

                loop {
                    // We stay here forever now and make sure this task will not stop until we
                    // receive the shutdown signal
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            });

            tokio::select! { _ = work => (), _ = signal => () };

            // Increment counter another time during shutdown
            counter.fetch_add(1, Ordering::Relaxed);

            Ok(())
        });

        manager.add("two", |_, _, _| async move {
            // Wait a little bit for the first task to do its work
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            panic!("This went wrong");
        });

        // Wait for panic to take place ..
        manager.on_exit().await;

        // .. then shut everything down
        manager.shutdown().await;

        // Check if we could do our work and shutdown procedure
        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }
}
