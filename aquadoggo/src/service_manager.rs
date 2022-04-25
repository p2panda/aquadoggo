// SPDX-License-Identifier: AGPL-3.0-or-later

use std::future::Future;

use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio::task;

#[async_trait::async_trait]
pub trait Service {
    async fn call(&self, tx: Sender<Message>, rx: Receiver<Message>);
}

#[async_trait::async_trait]
impl<FN, F> Service for FN
where
    FN: Fn(Sender<Message>, Receiver<Message>) -> F + Sync,
    F: Future<Output = ()> + Send + 'static,
{
    async fn call(&self, tx: Sender<Message>, rx: Receiver<Message>) {
        (self)(tx, rx).await
    }
}

#[derive(Clone, Debug)]
pub enum Message {
    GracefulShutdown,
}

pub struct ServiceManager {
    services: Vec<task::JoinHandle<()>>,
    tx: Sender<Message>,
}

impl ServiceManager {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = channel(capacity);

        Self {
            services: Vec::new(),
            tx,
        }
    }

    pub fn add<F: Service + Send + Sync + Copy + 'static>(&mut self, service: F) {
        let tx = self.tx.clone();
        let rx = tx.subscribe();

        task::spawn(async move {
            service.call(tx, rx).await;
        });
    }

    pub async fn shutdown(self) {
        let mut rx = self.tx.subscribe();

        // Broadcast graceful shutdown messages to all services
        self.tx.send(Message::GracefulShutdown).unwrap();

        // We drop our sender first to make sure _all_ senders get eventually closed, because the
        // recv() call otherwise sleeps forever.
        drop(self.tx);

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
    use tokio::sync::broadcast::{Receiver, Sender};

    use super::{Message, ServiceManager};

    #[tokio::test]
    async fn test() {
        let mut manager = ServiceManager::new(1024);

        manager.add(
            |tx: Sender<Message>, mut rx: Receiver<Message>| async move {
                let handle = tokio::task::spawn(async {
                    loop {
                        // Doing some very important work here
                        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                        println!("Important work");
                    }
                });

                tokio::select! {
                    _ = handle => {
                        println!("Important work finished");
                    }
                    Ok(Message::GracefulShutdown) = rx.recv() => {
                        println!("Received shutdown signal");
                    }
                };

                println!("Exit ..");

                // Some "work" we have to do before we can actually close this service
                tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
                println!("Done ..!");

                drop(tx);
            },
        );

        manager.shutdown().await;
    }
}
