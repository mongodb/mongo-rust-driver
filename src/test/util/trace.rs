use crate::runtime;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{field::Field, span, Level, Metadata};

/// Models the data reported in a tracing event.
#[derive(Debug, Clone)]
pub struct TracingEvent {
    /// The verbosity level.
    pub level: Level,
    /// The target, i.e. component the event corresponds to.
    pub target: String,
    /// Map of key/value pairs attached to the event.
    pub fields: std::collections::HashMap<String, TracingEventValue>,
}

impl TracingEvent {
    fn new(level: Level, target: String) -> TracingEvent {
        TracingEvent {
            level,
            target,
            fields: Default::default(),
        }
    }
}

/// Models the value of a field in a tracing event.
#[derive(Debug, Clone)]
pub enum TracingEventValue {
    F64(f64),
    I64(i64),
    U64(u64),
    Bool(bool),
    String(String),
}

/// A type for use in tests that need to consume tracing events. To use this type, initialize
/// a new instance with `TracingHandler::new()` and register it via `set_as_default_handler`.
/// The type will then listen for tracing events published by the driver and its dependencies
/// and broadcast them to a channel. To receive the broadcasted events, call `subscribe` to
/// create a new `TracingEventSubscriber`.
#[derive(Clone)]
pub struct TracingHandler {
    /// The maximum verbosity level which this handler will process and broadcast events for.
    max_verbosity_level: Level,
    /// Sender for the channel where events will be broadcast.
    event_broadcaster: broadcast::Sender<TracingEvent>,
}

impl TracingHandler {
    pub fn new(max_verbosity_level: Level) -> TracingHandler {
        let (event_broadcaster, _) = tokio::sync::broadcast::channel(10_000);
        Self {
            max_verbosity_level,
            event_broadcaster,
        }
    }

    /// Installs this via `tracing` as the default handler for tracing events until the returned
    /// guard is dropped.
    pub fn set_as_default(&self) -> tracing::subscriber::DefaultGuard {
        tracing::subscriber::set_default(self.clone())
    }

    /// Returns a `TracingSubscriber` that will listen for tracing events broadcast by this handler.
    pub fn subscribe(&self) -> TracingSubscriber {
        TracingSubscriber {
            _handler: self,
            receiver: self.event_broadcaster.subscribe(),
        }
    }
}

/// Implementation allowing `TracingHandler` to subscribe to `tracing` events.
impl tracing::Subscriber for TracingHandler {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() >= &self.max_verbosity_level
    }

    fn event(&self, event: &tracing::Event<'_>) {
        let mut test_event = TracingEvent::new(
            *event.metadata().level(),
            event.metadata().target().to_string(),
        );
        let mut visitor = TracingEventVisitor::new(&mut test_event);
        event.record(&mut visitor);
        // this only errors if no receivers are listening; we don't care if that is the case.
        let _: std::result::Result<usize, broadcast::error::SendError<TracingEvent>> =
            self.event_broadcaster.send(test_event);
    }

    /// These methods all relate to spans. Since we don't create any spans ourselves or need
    /// to make any assertions about them, we do not need real implementations.
    fn new_span(&self, _span: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }
    fn record(&self, _span: &span::Id, _values: &span::Record<'_>) {}
    fn record_follows_from(&self, _span: &span::Id, _follows: &span::Id) {}
    fn enter(&self, _span: &span::Id) {}
    fn exit(&self, _span: &span::Id) {}
}

pub struct TracingSubscriber<'a> {
    /// A reference to the handler this subscriber is receiving events from.
    /// Stored here to ensure this subscriber cannot outlive the handler that is generating its
    /// events.
    _handler: &'a TracingHandler,
    /// Receiver for the channel where `_handler` braodcasts events.
    receiver: broadcast::Receiver<TracingEvent>,
}

impl TracingSubscriber<'_> {
    /// Waits up to `timeout` for an event matching the specified filter. Returns a matching event
    /// if one is found, or otherwise None.
    pub async fn wait_for_event<F>(&mut self, timeout: Duration, filter: F) -> Option<TracingEvent>
    where
        F: Fn(&TracingEvent) -> bool,
    {
        runtime::timeout(timeout, async {
            loop {
                match self.receiver.recv().await {
                    Ok(event) if filter(&event) => return event.into(),
                    // the channel hit capacity and missed some events.
                    Err(broadcast::error::RecvError::Lagged(amount_skipped)) => {
                        panic!("receiver lagged and skipped {} events", amount_skipped)
                    }
                    Err(_) => return None,
                    _ => continue,
                }
            }
        })
        .await
        .ok()
        .flatten()
    }

    /// Collects events matching the specified filter. Returns once `timeout` has passed without
    /// a matching event occurring.
    pub async fn collect_events<F>(&mut self, timeout: Duration, filter: F) -> Vec<TracingEvent>
    where
        F: Fn(&TracingEvent) -> bool,
    {
        let mut events = Vec::new();
        while let Some(event) = self.wait_for_event(timeout, &filter).await {
            events.push(event);
        }
        events
    }
}

/// A visitor which traverses each value in a tracing event and stores it in the underlying
/// `TracingEvent`.
struct TracingEventVisitor<'a> {
    event: &'a mut TracingEvent,
}

impl TracingEventVisitor<'_> {
    fn new(event: &mut TracingEvent) -> TracingEventVisitor {
        TracingEventVisitor { event }
    }
}

impl tracing::field::Visit for TracingEventVisitor<'_> {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.event
            .fields
            .insert(field.name().to_string(), TracingEventValue::F64(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.event
            .fields
            .insert(field.name().to_string(), TracingEventValue::I64(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.event
            .fields
            .insert(field.name().to_string(), TracingEventValue::U64(value));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.event
            .fields
            .insert(field.name().to_string(), TracingEventValue::Bool(value));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.event.fields.insert(
            field.name().to_string(),
            TracingEventValue::String(value.to_string()),
        );
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.event.fields.insert(
            field.name().to_string(),
            TracingEventValue::String(format!("{:?}", value)),
        );
    }
}
