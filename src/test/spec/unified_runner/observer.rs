use tokio::sync::{
    broadcast::{
        self,
        error::{RecvError, TryRecvError},
    },
    RwLock,
};

use std::{sync::Arc, time::Duration};

use crate::{
    error::{Error, Result},
    runtime,
    test::Event,
};

use super::{events_match, EntityMap, ExpectedEvent};

// TODO: RUST-1424: consolidate this with `EventHandler`
/// Observer used to cache all the seen events for a given client in a unified test.
/// Used to implement assertEventCount and waitForEvent operations.
#[derive(Debug)]
pub(crate) struct EventObserver {
    seen_events: Vec<Event>,
    receiver: broadcast::Receiver<Event>,
}

impl EventObserver {
    pub fn new(receiver: broadcast::Receiver<Event>) -> Self {
        Self {
            seen_events: Vec::new(),
            receiver,
        }
    }

    pub(crate) async fn recv(&mut self) -> Option<Event> {
        match self.receiver.recv().await {
            Ok(e) => {
                self.seen_events.push(e.clone());
                Some(e)
            }
            Err(RecvError::Lagged(_)) => panic!("event receiver lagged"),
            Err(RecvError::Closed) => None,
        }
    }

    fn try_recv(&mut self) -> Option<Event> {
        match self.receiver.try_recv() {
            Ok(e) => {
                self.seen_events.push(e.clone());
                Some(e)
            }
            Err(TryRecvError::Lagged(_)) => panic!("event receiver lagged"),
            Err(TryRecvError::Closed | TryRecvError::Empty) => None,
        }
    }

    pub(crate) fn matching_events(
        &mut self,
        event: &ExpectedEvent,
        entities: &EntityMap,
    ) -> Vec<Event> {
        // first retrieve all the events buffered in the channel
        while self.try_recv().is_some() {}
        // Then collect all matching events.
        self.seen_events
            .iter()
            .filter(|e| events_match(e, event, Some(&entities)).is_ok())
            .cloned()
            .collect()
    }

    pub(crate) async fn wait_for_matching_events(
        &mut self,
        event: &ExpectedEvent,
        count: usize,
        entities: Arc<RwLock<EntityMap>>,
    ) -> Result<()> {
        let mut seen = self.matching_events(event, &*entities.read().await).len();

        if seen >= count {
            return Ok(());
        }

        runtime::timeout(Duration::from_secs(10), async {
            while let Some(e) = self.recv().await {
                let es = entities.read().await;
                if events_match(&e, event, Some(&es)).is_ok() {
                    seen += 1;
                    if seen == count {
                        return Ok(());
                    }
                }
            }
            Err(Error::internal(format!(
                "ran out of events before, only saw {} of {}",
                seen, count
            )))
        })
        .await??;

        Ok(())
    }
}
