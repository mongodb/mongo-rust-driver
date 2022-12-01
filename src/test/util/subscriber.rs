use std::time::Duration;

use crate::runtime;

use tokio::sync::broadcast::{error::RecvError, Receiver};

/// A generic subscriber type that can be used to subscribe to events from any handler type
/// that publishes events to a broadcast channel.
#[derive(Debug)]
pub(crate) struct EventSubscriber<'a, H, E: Clone> {
    /// A reference to the handler this subscriber is receiving events from.
    /// Stored here to ensure this subscriber cannot outlive the handler that is generating its
    /// events.
    _handler: &'a H,
    receiver: Receiver<E>,
}

impl<H, E: Clone> EventSubscriber<'_, H, E> {
    pub(crate) fn new(handler: &H, receiver: Receiver<E>) -> EventSubscriber<'_, H, E> {
        EventSubscriber {
            _handler: handler,
            receiver,
        }
    }

    pub(crate) async fn wait_for_event<F>(&mut self, timeout: Duration, mut filter: F) -> Option<E>
    where
        F: FnMut(&E) -> bool,
    {
        self.filter_map_event(timeout, |e| if filter(&e) { Some(e) } else { None })
            .await
    }

    pub(crate) async fn collect_events<F>(&mut self, timeout: Duration, mut filter: F) -> Vec<E>
    where
        F: FnMut(&E) -> bool,
    {
        let mut events = Vec::new();
        let _ = runtime::timeout(timeout, async {
            while let Some(event) = self.wait_for_event(timeout, &mut filter).await {
                events.push(event);
            }
        })
        .await;
        events
    }

    #[cfg(feature = "in-use-encryption-unstable")]
    pub(crate) async fn collect_events_map<F, T>(
        &mut self,
        timeout: Duration,
        mut filter: F,
    ) -> Vec<T>
    where
        F: FnMut(E) -> Option<T>,
    {
        let mut events = Vec::new();
        let _ = runtime::timeout(timeout, async {
            while let Some(event) = self.filter_map_event(timeout, &mut filter).await {
                events.push(event);
            }
        })
        .await;
        events
    }

    #[cfg(feature = "in-use-encryption-unstable")]
    pub(crate) async fn clear_events(&mut self, timeout: Duration) {
        self.collect_events(timeout, |_| true).await;
    }

    /// Consume and pass events to the provided closure until it returns Some or the timeout is hit.
    pub(crate) async fn filter_map_event<F, T>(
        &mut self,
        timeout: Duration,
        mut filter_map: F,
    ) -> Option<T>
    where
        F: FnMut(E) -> Option<T>,
    {
        runtime::timeout(timeout, async {
            loop {
                match self.receiver.recv().await {
                    Ok(event) => {
                        if let Some(e) = filter_map(event) {
                            return Some(e);
                        } else {
                            continue;
                        }
                    }
                    // the channel hit capacity and missed some events.
                    Err(RecvError::Lagged(amount_skipped)) => {
                        panic!("receiver lagged and skipped {} events", amount_skipped)
                    }
                    Err(_) => return None,
                }
            }
        })
        .await
        .ok()
        .flatten()
    }

    /// Returns the received events without waiting for any more.
    pub(crate) fn all<F>(&mut self, filter: F) -> Vec<E>
    where
        F: Fn(&E) -> bool,
    {
        let mut events = Vec::new();
        while let Ok(event) = self.receiver.try_recv() {
            if filter(&event) {
                events.push(event);
            }
        }
        events
    }
}
