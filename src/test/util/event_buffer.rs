use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use time::OffsetDateTime;
use tokio::sync::Notify;

use crate::{
    client::options::ClientOptions,
    event::{
        cmap::CmapEvent,
        command::{CommandEvent, CommandStartedEvent, CommandSucceededEvent},
    },
    runtime,
};

use super::Event;

/// A buffer of events that provides utility methods for querying the buffer and awaiting new event
/// arrival.
///
/// If working with events one at a time as they arrive is more convenient for a particular test,
/// register an `EventBuffer` with the `Client` and then use `EventBuffer::stream` or
/// `EventBuffer::stream_all`.
#[derive(Clone, Debug)]
pub(crate) struct EventBuffer<T = Event> {
    inner: Arc<EventBufferInner<T>>,
}

#[derive(Debug)]
struct EventBufferInner<T> {
    events: Mutex<GenVec<(T, OffsetDateTime)>>,
    event_received: Notify,
}

#[derive(Debug)]
struct GenVec<T> {
    data: Vec<T>,
    generation: Generation,
}

#[derive(Copy, Clone, PartialEq, Debug)]
struct Generation(usize);

impl<T> GenVec<T> {
    fn new() -> Self {
        Self {
            data: vec![],
            generation: Generation(0),
        }
    }
}

impl<T> EventBuffer<T> {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(EventBufferInner {
                events: Mutex::new(GenVec::new()),
                event_received: Notify::new(),
            }),
        }
    }

    pub(crate) fn filter_map<R>(&self, f: impl Fn(&T) -> Option<R>) -> Vec<R> {
        self.inner
            .events
            .lock()
            .unwrap()
            .data
            .iter()
            .map(|(ev, _)| ev)
            .filter_map(f)
            .collect()
    }

    /// Create a new stream view of events buffered after the point of this call.
    pub(crate) fn stream(&self) -> EventStream<'_, T> {
        let (index, generation) = {
            let events = self.inner.events.lock().unwrap();
            (events.data.len(), events.generation)
        };
        EventStream {
            buffer: self,
            index,
            generation,
        }
    }

    /// Create a new stream view, starting with events already contained in the buffer.
    pub(crate) fn stream_all(&self) -> EventStream<'_, T> {
        EventStream {
            buffer: self,
            index: 0,
            generation: self.inner.events.lock().unwrap().generation,
        }
    }

    // The `mut` isn't necessary on `self` here, but it serves as a useful lint on those
    // methods that modify; if the caller only has a `&EventHandler` it can at worst case
    // `clone` to get a `mut` one.
    fn invalidate<R>(&mut self, f: impl FnOnce(&mut Vec<(T, OffsetDateTime)>) -> R) -> R {
        let mut events = self.inner.events.lock().unwrap();
        events.generation = Generation(events.generation.0 + 1);
        let out = f(&mut events.data);
        self.inner.event_received.notify_waiters();
        out
    }

    /// Clear all cached events.  This will cause any derived `EventStream`s to error.
    pub(crate) fn clear_cached_events(&mut self) {
        self.invalidate(|data| data.clear());
    }

    /// Remove all cached events that don't match the predicate.  This will cause any derived
    /// `EventStream`s to error.
    pub(crate) fn retain(&mut self, mut f: impl FnMut(&T) -> bool) {
        self.invalidate(|data| data.retain(|(ev, _)| f(ev)));
    }

    pub(crate) fn push_event(&self, ev: T) {
        self.inner
            .events
            .lock()
            .unwrap()
            .data
            .push((ev, OffsetDateTime::now_utc()));
        self.inner.event_received.notify_waiters();
    }
}

impl<T: Clone> EventBuffer<T> {
    /// Returns a list of current events.
    pub(crate) fn all(&self) -> Vec<T> {
        self.all_timed().into_iter().map(|(ev, _)| ev).collect()
    }

    pub(crate) fn all_timed(&self) -> Vec<(T, OffsetDateTime)> {
        self.inner.events.lock().unwrap().data.clone()
    }
}

impl<T: Clone + Send + Sync + 'static> EventBuffer<T> {
    pub(crate) fn handler<V: Into<T> + Send + Sync + 'static>(
        &self,
    ) -> crate::event::EventHandler<V> {
        let this = self.clone();
        crate::event::EventHandler::callback(move |ev: V| this.push_event(ev.into()))
    }
}

impl EventBuffer<Event> {
    pub(crate) fn register(&self, client_options: &mut ClientOptions) {
        client_options.command_event_handler = Some(self.handler());
        client_options.sdam_event_handler = Some(self.handler());
        client_options.cmap_event_handler = Some(self.handler());
    }

    pub(crate) fn connections_checked_out(&self) -> u32 {
        let mut count = 0;
        for (ev, _) in self.inner.events.lock().unwrap().data.iter() {
            match ev {
                Event::Cmap(CmapEvent::ConnectionCheckedOut(_)) => count += 1,
                Event::Cmap(CmapEvent::ConnectionCheckedIn(_)) => count -= 1,
                _ => (),
            }
        }
        count
    }

    /// Gets all of the command started events for the specified command names.
    pub(crate) fn get_command_started_events(
        &self,
        command_names: &[&str],
    ) -> Vec<CommandStartedEvent> {
        self.inner
            .events
            .lock()
            .unwrap()
            .data
            .iter()
            .filter_map(|(event, _)| match event {
                Event::Command(CommandEvent::Started(event)) => {
                    if command_names.contains(&event.command_name.as_str()) {
                        Some(event.clone())
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect()
    }

    /// Gets all of the command started events, excluding configureFailPoint events.
    pub(crate) fn get_all_command_started_events(&self) -> Vec<CommandStartedEvent> {
        self.inner
            .events
            .lock()
            .unwrap()
            .data
            .iter()
            .filter_map(|(event, _)| match event {
                Event::Command(CommandEvent::Started(event))
                    if event.command_name != "configureFailPoint" =>
                {
                    Some(event.clone())
                }
                _ => None,
            })
            .collect()
    }

    /// Gets all command events matching any of the command names.
    pub(crate) fn get_command_events(&self, command_names: &[&str]) -> Vec<CommandEvent> {
        self.filter_map(|e| match e {
            Event::Command(cev) if command_names.iter().any(|&n| n == cev.command_name()) => {
                Some(cev.clone())
            }
            _ => None,
        })
    }

    /// Gets the first started/succeeded pair of events for the given command name.
    ///
    /// Panics if the command failed or could not be found in the events.
    pub(crate) fn get_successful_command_execution(
        &self,
        command_name: &str,
    ) -> (CommandStartedEvent, CommandSucceededEvent) {
        let cevs = self.get_command_events(&[command_name]);
        if cevs.len() < 2 {
            panic!("too few command events for {:?}: {:?}", command_name, cevs);
        }
        match &cevs[0..2] {
            [CommandEvent::Started(started), CommandEvent::Succeeded(succeeded)] => {
                (started.clone(), succeeded.clone())
            }
            pair => panic!(
                "First event pair for {:?} not (Started, Succeded): {:?}",
                command_name, pair
            ),
        }
    }

    pub(crate) fn count_pool_cleared_events(&self) -> usize {
        let mut out = 0;
        for event in self.all().iter() {
            if matches!(event, Event::Cmap(CmapEvent::PoolCleared(_))) {
                out += 1;
            }
        }
        out
    }
}

/// Iterate one at a time over events in an `EventBuffer`.
pub(crate) struct EventStream<'a, T> {
    buffer: &'a EventBuffer<T>,
    index: usize,
    generation: Generation,
}

impl<'a, T: Clone> EventStream<'a, T> {
    fn try_next(&mut self) -> Option<T> {
        let events = self.buffer.inner.events.lock().unwrap();
        if events.generation != self.generation {
            panic!("EventBuffer cleared during EventStream iteration");
        }
        if events.data.len() > self.index {
            let event = events.data[self.index].0.clone();
            self.index += 1;
            return Some(event);
        }
        None
    }

    /// Get the next unread event from the underlying buffer, waiting for a new one to arrive if all
    /// current ones have been read.
    pub(crate) async fn next(&mut self, timeout: Duration) -> Option<T> {
        crate::runtime::timeout(timeout, async move {
            loop {
                let notified = self.buffer.inner.event_received.notified();
                if let Some(next) = self.try_next() {
                    return Some(next);
                }
                notified.await;
            }
        })
        .await
        .unwrap_or(None)
    }

    /// Get the next unread event for which the provided closure returnes `Some`, waiting for new
    /// events to arrive if all current ones have been read.
    pub(crate) async fn next_map<F, R>(&mut self, timeout: Duration, mut filter_map: F) -> Option<R>
    where
        F: FnMut(T) -> Option<R>,
    {
        runtime::timeout(timeout, async move {
            loop {
                let ev = self.next(timeout).await?;
                if let Some(r) = filter_map(ev) {
                    return Some(r);
                }
            }
        })
        .await
        .unwrap_or(None)
    }

    /// Get the next unread event for which the provided closure returnes `true`, waiting for new
    /// events to arrive if all current ones have been read.
    pub(crate) async fn next_match<F>(&mut self, timeout: Duration, mut filter: F) -> Option<T>
    where
        F: FnMut(&T) -> bool,
    {
        self.next_map(timeout, |e| if filter(&e) { Some(e) } else { None })
            .await
    }

    /// Collect all unread events matching a predicate, waiting up to a timeout for additional ones
    /// to arrive.
    pub(crate) async fn collect<F>(&mut self, timeout: Duration, mut filter: F) -> Vec<T>
    where
        F: FnMut(&T) -> bool,
    {
        let mut events = Vec::new();
        let _ = runtime::timeout(timeout, async {
            while let Some(event) = self.next_match(timeout, &mut filter).await {
                events.push(event);
            }
        })
        .await;
        events
    }

    #[cfg(feature = "in-use-encryption")]
    pub(crate) async fn collect_map<F, R>(&mut self, timeout: Duration, mut filter: F) -> Vec<R>
    where
        F: FnMut(T) -> Option<R>,
    {
        let mut events = Vec::new();
        let _ = runtime::timeout(timeout, async {
            while let Some(event) = self.next_map(timeout, &mut filter).await {
                events.push(event);
            }
        })
        .await;
        events
    }

    /// Collects all unread events matching a predicate without waiting for any more.
    pub(crate) fn collect_now<F>(&mut self, filter: F) -> Vec<T>
    where
        F: Fn(&T) -> bool,
    {
        let events = self.buffer.inner.events.lock().unwrap();
        if events.generation != self.generation {
            panic!("EventBuffer cleared during EventStream iteration");
        }
        let out = events
            .data
            .iter()
            .skip(self.index)
            .map(|(e, _)| e)
            .filter(|e| filter(*e))
            .cloned()
            .collect();
        self.index = events.data.len();
        out
    }
}

impl<'a> EventStream<'a, Event> {
    /// Gets the next unread CommandStartedEvent/CommandFailedEvent pair.
    /// If the next CommandStartedEvent is associated with a CommandFailedEvent, this method will
    /// panic.
    pub(crate) async fn next_successful_command_execution(
        &mut self,
        timeout: Duration,
        command_name: impl AsRef<str>,
    ) -> Option<(CommandStartedEvent, CommandSucceededEvent)> {
        runtime::timeout(timeout, async {
            let started = self
                .next_map(Duration::MAX, |event| match event {
                    Event::Command(CommandEvent::Started(s))
                        if s.command_name == command_name.as_ref() =>
                    {
                        Some(s)
                    }
                    _ => None,
                })
                .await
                .unwrap();

            let succeeded = self
                .next_map(Duration::MAX, |event| match event {
                    Event::Command(CommandEvent::Succeeded(s))
                        if s.request_id == started.request_id =>
                    {
                        Some(s)
                    }
                    Event::Command(CommandEvent::Failed(f))
                        if f.request_id == started.request_id =>
                    {
                        panic!(
                            "expected {} to succeed but it failed: {:#?}",
                            command_name.as_ref(),
                            f
                        )
                    }
                    _ => None,
                })
                .await
                .unwrap();

            (started, succeeded)
        })
        .await
        .ok()
    }

    pub(crate) async fn collect_successful_command_execution(
        &mut self,
        timeout: Duration,
        command_name: impl AsRef<str>,
    ) -> Vec<(CommandStartedEvent, CommandSucceededEvent)> {
        let mut event_pairs = Vec::new();
        let command_name = command_name.as_ref();
        let _ = runtime::timeout(timeout, async {
            while let Some(next_pair) = self
                .next_successful_command_execution(timeout, command_name)
                .await
            {
                event_pairs.push(next_pair);
            }
        })
        .await;
        event_pairs
    }
}
