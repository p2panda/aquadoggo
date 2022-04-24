// SPDX-License-Identifier: AGPL-3.0-or-later

use std::future::Future;

use tokio::task;

#[async_trait::async_trait]
pub trait Service {
    async fn call(&self);
}

#[async_trait::async_trait]
impl<FN, F> Service for FN
where
    FN: Fn() -> F + Sync,
    F: Future<Output = ()> + Send + 'static,
{
    async fn call(&self) {
        (self)().await
    }
}

pub struct ServiceManager {
    services: Vec<task::JoinHandle<()>>,
}

impl ServiceManager {
    pub fn new() -> Self {
        Self {
            services: Vec::new(),
        }
    }

    pub fn add<F: Service + Send + Sync + Copy + 'static>(&mut self, service: F) {
        task::spawn(async move {
            service.call().await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::ServiceManager;

    #[tokio::test]
    async fn test() {
        let mut manager = ServiceManager::new();

        manager.add(|| async {});
    }
}
