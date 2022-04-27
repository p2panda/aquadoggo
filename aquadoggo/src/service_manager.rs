// SPDX-License-Identifier: AGPL-3.0-or-later

use std::future::Future;

use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::task;
use tokio::task::JoinHandle;

pub type Sender = broadcast::Sender<Message>;
pub type Shutdown = broadcast::Receiver<bool>;

#[async_trait::async_trait]
pub trait Service {
    async fn call(&self, shutdown: Shutdown, tx: Sender);
}

#[async_trait::async_trait]
impl<FN, F> Service for FN
where
    FN: Fn(Shutdown, Sender) -> F + Sync,
    F: Future<Output = ()> + Send + 'static,
{
    async fn call(&self, shutdown: Shutdown, tx: Sender) {
        (self)(shutdown, tx).await
    }
}

#[derive(Clone, Debug)]
pub enum Message {
    GracefulShutdown,
}

pub struct ServiceManager {
    services: Vec<task::JoinHandle<()>>,
    tx: Sender,
    shutdown: broadcast::Sender<bool>,
}

impl ServiceManager {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        let (shutdown, _) = broadcast::channel(16);

        Self {
            services: Vec::new(),
            tx,
            shutdown,
        }
    }

    pub fn add<F: Service + Send + Sync + Copy + 'static>(&mut self, service: F) {
        let tx = self.tx.clone();
        let shutdown_tx = self.shutdown.clone();
        let shutdown_rx = shutdown_tx.subscribe();

        task::spawn(async move {
            // Run the service!
            service.call(shutdown_rx, tx).await;

            // Drop the sender of this service when we're done
            drop(shutdown_tx);
        });
    }

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

pub async fn done_or_shutdown<T>(handle: JoinHandle<T>, mut shutdown: Shutdown) {
    tokio::select! {
        _ = handle => {
            // Service work finished
        }
        _ = shutdown.recv() => {
            // Received shutdown signal
        }
    };
}

#[cfg(test)]
mod tests {
    use super::{done_or_shutdown, ServiceManager, Shutdown};

    #[tokio::test]
    async fn service_manager() {
        let mut manager = ServiceManager::new(1024);

        manager.add(|signal: Shutdown, _| async move {
            let work = tokio::task::spawn(async {
                loop {
                    // Doing some very important work here
                }
            });

            done_or_shutdown(work, signal).await;

            // Some "tidying" we have to do before we can actually close this service
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        });

        manager.shutdown().await;
    }
}
