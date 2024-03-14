use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use time::OffsetDateTime;
use tokio::sync::{futures::Notified, Notify};

use crate::{
    client::options::ClientOptions,
    event::{
        cmap::CmapEvent,
        command::{CommandEvent, CommandStartedEvent, CommandSucceededEvent},
    },
    runtime,
};

use super::Event;

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

    #[allow(unused)]
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

    /// Subscribe to events generated after the point of this call.
    pub(crate) fn subscribe(&self) -> EventSubscriber<'_, T> {
        let (index, generation) = {
            let events = self.inner.events.lock().unwrap();
            (events.data.len(), events.generation)
        };
        EventSubscriber {
            buffer: self,
            index,
            generation,
        }
    }

    /// Subscribe to all events contained in the buffer.
    pub(crate) fn subscribe_all(&self) -> EventSubscriber<'_, T> {
        EventSubscriber {
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

    pub(crate) fn clear_cached_events(&mut self) {
        self.invalidate(|data| data.clear());
    }

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
    /// Returns a list of current events and a future to await for more being received.
    pub(crate) fn all(&self) -> (Vec<T>, Notified) {
        // The `Notify` must be created *before* reading the events to ensure any added
        // events trigger notifications.
        let notify = self.inner.event_received.notified();
        let events = self
            .inner
            .events
            .lock()
            .unwrap()
            .data
            .iter()
            .map(|(ev, _)| ev)
            .cloned()
            .collect();
        (events, notify)
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

    /// Remove all command events from the buffer, returning those matching any of the command
    /// names.
    pub(crate) fn get_command_events(&mut self, command_names: &[&str]) -> Vec<CommandEvent> {
        let mut out = vec![];
        self.retain(|ev| match ev {
            Event::Command(cev) => {
                if command_names.contains(&cev.command_name()) {
                    out.push(cev.clone());
                }
                false
            }
            _ => true,
        });
        out
    }

    /// Gets the first started/succeeded pair of events for the given command name, popping off all
    /// command events before and between them.
    ///
    /// Panics if the command failed or could not be found in the events.
    pub(crate) fn get_successful_command_execution(
        &mut self,
        command_name: &str,
    ) -> (CommandStartedEvent, CommandSucceededEvent) {
        let mut started = None;
        let mut succeeded = None;
        self.retain(|ev| match (ev, &started, &succeeded) {
            (Event::Command(cev), None, None) => {
                if cev.command_name() == command_name {
                    started = Some(
                        cev.as_command_started()
                            .unwrap_or_else(|| {
                                panic!("first event not a command started event {:?}", cev)
                            })
                            .clone(),
                    );
                }
                false
            }
            (Event::Command(cev), Some(started), None) => {
                if cev.request_id() == started.request_id {
                    succeeded = Some(
                        cev.as_command_succeeded()
                            .expect("second event not a command succeeded event")
                            .clone(),
                    );
                }
                false
            }
            _ => true,
        });
        return match (started, succeeded) {
            (Some(started), Some(succeeded)) => (started, succeeded),
            _ => panic!("could not find event for {} command", command_name),
        };
    }

    pub(crate) fn count_pool_cleared_events(&self) -> usize {
        let mut out = 0;
        for event in self.all().0.iter() {
            if matches!(event, Event::Cmap(CmapEvent::PoolCleared(_))) {
                out += 1;
            }
        }
        out
    }
}

pub(crate) struct EventSubscriber<'a, T> {
    buffer: &'a EventBuffer<T>,
    index: usize,
    generation: Generation,
}

impl<'a, T: Clone> EventSubscriber<'a, T> {
    async fn next(&mut self, timeout: Duration) -> Option<T> {
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

    /// Consume and pass events to the provided closure until it returns Some or the timeout is hit.
    pub(crate) async fn filter_map_event<F, R>(
        &mut self,
        timeout: Duration,
        mut filter_map: F,
    ) -> Option<R>
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

    /// Waits for an event to occur within the given duration that passes the given filter.
    pub(crate) async fn wait_for_event<F>(&mut self, timeout: Duration, mut filter: F) -> Option<T>
    where
        F: FnMut(&T) -> bool,
    {
        self.filter_map_event(timeout, |e| if filter(&e) { Some(e) } else { None })
            .await
    }

    pub(crate) async fn collect_events<F>(&mut self, timeout: Duration, mut filter: F) -> Vec<T>
    where
        F: FnMut(&T) -> bool,
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
    pub(crate) async fn collect_events_map<F, R>(
        &mut self,
        timeout: Duration,
        mut filter: F,
    ) -> Vec<R>
    where
        F: FnMut(T) -> Option<R>,
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

    /// Returns the received events without waiting for any more.
    pub(crate) fn all<F>(&mut self, filter: F) -> Vec<T>
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

impl<'a> EventSubscriber<'a, Event> {
    /// Waits for the next CommandStartedEvent/CommandFailedEvent pair.
    /// If the next CommandStartedEvent is associated with a CommandFailedEvent, this method will
    /// panic.
    pub(crate) async fn wait_for_successful_command_execution(
        &mut self,
        timeout: Duration,
        command_name: impl AsRef<str>,
    ) -> Option<(CommandStartedEvent, CommandSucceededEvent)> {
        runtime::timeout(timeout, async {
            let started = self
                .filter_map_event(Duration::MAX, |event| match event {
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
                .filter_map_event(Duration::MAX, |event| match event {
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
}
