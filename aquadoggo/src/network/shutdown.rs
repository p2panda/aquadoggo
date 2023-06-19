// SPDX-License-Identifier: AGPL-3.0-or-later

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{FutureExt, Stream};
use triggered::{Listener, Trigger};

/// Helper to coordinate finishing an async process which needs to take place before we can close
/// the application.
#[derive(Clone)]
pub struct ShutdownHandler {
    request_trigger: Trigger,
    request_signal: Listener,
    done_trigger: Trigger,
    done_signal: Listener,
}

impl ShutdownHandler {
    /// Returns a new instance of `ShutdownHandler`.
    pub fn new() -> Self {
        let (request_trigger, request_signal) = triggered::trigger();
        let (done_trigger, done_signal) = triggered::trigger();

        Self {
            request_trigger,
            request_signal,
            done_trigger,
            done_signal,
        }
    }

    /// Returns an async stream which can be polled to find out if a shutdown request was sent.
    pub fn is_requested(&self) -> ShutdownRequest {
        ShutdownRequest {
            inner: self.request_signal.clone(),
            is_sent: false,
        }
    }

    /// Signal that the shutdown has completed.
    pub fn set_done(&mut self) {
        self.done_trigger.trigger();
    }

    /// Returns a future which can be polled to find out if the shutdown has completed.
    ///
    /// This automatically triggers the request to shut down when being called.
    pub fn is_done(&mut self) -> Listener {
        self.request_trigger.trigger();
        self.done_signal.clone()
    }
}

pub struct ShutdownRequest {
    inner: Listener,
    is_sent: bool,
}

impl Stream for ShutdownRequest {
    type Item = bool;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_sent {
            return Poll::Pending;
        }

        match self.inner.poll_unpin(cx) {
            Poll::Ready(_) => {
                self.is_sent = true;
                Poll::Ready(Some(true))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use tokio_stream::StreamExt;

    use super::ShutdownHandler;

    #[tokio::test]
    async fn changes_value_before_shutdown() {
        let num = Arc::new(AtomicUsize::new(0));
        let mut handler = ShutdownHandler::new();

        {
            let num = num.clone();
            let mut handler = handler.clone();

            tokio::task::spawn(async move {
                let mut signal = handler.is_requested();

                loop {
                    tokio::select! {
                        _ = signal.next() => {
                            // Change the value before we wind down
                            num.store(100, Ordering::Relaxed);
                            handler.set_done();
                        }
                    };
                }
            });
        }

        handler.is_done().await;

        assert_eq!(num.load(Ordering::Relaxed), 100);
    }
}
