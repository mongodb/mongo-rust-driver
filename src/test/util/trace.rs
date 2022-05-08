use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
};
use tracing::{field::Field, span, Event, Level, Metadata, Subscriber};

/// Models the data reported in a tracing event.

#[derive(Debug, Clone)]
pub struct TracingEvent {
    /// The verbosity level.
    pub level: Level,
    /// The target, i.e. component the event corresponds to.
    pub target: String,
    /// Map of key/value pairs attached to the event.
    pub fields: HashMap<String, TracingEventValue>,
}

impl TracingEvent {
    fn new(level: Level, target: String) -> TracingEvent {
        TracingEvent {
            level,
            target,
            fields: HashMap::new(),
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

/// A tracing subscriber for use in tests.
#[derive(Clone)]
pub struct TracingSubscriber {
    /// The maximum verbosity level which this subscriber will
    /// collect events for.
    max_verbosity_level: Level,
    /// Stores events the subscriber has collected.
    events: Arc<RwLock<VecDeque<TracingEvent>>>,
}

impl TracingSubscriber {
    pub fn new(max_verbosity_level: Level) -> TracingSubscriber {
        TracingSubscriber {
            max_verbosity_level,
            events: Default::default(),
        }
    }

    /// Retrieves all of the tracing events collected by the subscriber.
    pub fn get_all_events(&self) -> Vec<TracingEvent> {
        let events = self.events.read().unwrap();
        events.iter().cloned().collect()
    }

    /// Installs this subscriber as the default until the returned guard is dropped.
    pub fn set_as_default(&self) -> tracing::subscriber::DefaultGuard {
        tracing::subscriber::set_default(self.clone())
    }
}

impl Subscriber for TracingSubscriber {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() >= &self.max_verbosity_level
    }

    fn event(&self, event: &Event<'_>) {
        let mut test_event = TracingEvent::new(
            *event.metadata().level(),
            event.metadata().target().to_string(),
        );
        let mut visitor = TracingEventVisitor::new(&mut test_event);
        event.record(&mut visitor);
        self.events.write().unwrap().push_back(test_event);
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
