use crate::test::spec::unified_runner::{test_file::TestCase, TestFileEntity};
use serde::Serialize;
use std::{
    collections::HashMap,
    convert::TryInto,
    sync::{Arc, RwLock},
};
use tracing::{field::Field, span, subscriber::Interest, Level, Metadata};

use super::event_buffer::{EventBuffer, EventStream};

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
            fields: Default::default(),
        }
    }

    /// Retrieves the topology_id value for the event. Panics if there is no topology_id or if the
    /// topology_id is not a string.
    pub(crate) fn topology_id(&self) -> String {
        self.get_value_as_string("topologyId")
    }

    /// Retrieves the field with the specified name as a string. Panics if the name is missing or
    /// or is not a string.
    pub fn get_value_as_string(&self, field: &'static str) -> String {
        match self.fields.get(field) {
            Some(TracingEventValue::String(s)) => s.to_string(),
            Some(v) => panic!("field {} was unexpectedly not a string: got {:?}", field, v),
            None => panic!("field {} was unexpectedly None", field),
        }
    }
}

/// Models the value of a field in a tracing event.
#[derive(Debug, Clone)]
pub enum TracingEventValue {
    F64(f64),
    I64(i64),
    U64(u64),
    I128(i128),
    U128(u128),
    Bool(bool),
    String(String),
}

/// Used for serializing tracing event data to BSON for the purpose of matching against expected
/// values.
impl Serialize for TracingEventValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            TracingEventValue::F64(v) => serializer.serialize_f64(*v),
            TracingEventValue::I64(v) => serializer.serialize_i64(*v),
            TracingEventValue::U64(v) => serializer.serialize_u64(*v),
            TracingEventValue::I128(v) => match (*v).try_into() {
                Ok(i) => serializer.serialize_i64(i),
                Err(e) => Err(serde::ser::Error::custom(format!(
                    "Failed to serialize i128 as i64: {}",
                    e
                ))),
            },
            TracingEventValue::U128(v) => match (*v).try_into() {
                Ok(i) => serializer.serialize_u64(i),
                Err(e) => Err(serde::ser::Error::custom(format!(
                    "Failed to serialize u128 as u64: {}",
                    e
                ))),
            },
            TracingEventValue::Bool(v) => serializer.serialize_bool(*v),
            TracingEventValue::String(v) => serializer.serialize_str(v.as_str()),
        }
    }
}

/// A type for use in tests that need to consume tracing events. For single-threaded tests,
/// this type may be created directly in the test using the desired severity levels via
/// [`TracingHandler::new_with_levels`] and used as a local default via
/// [`tracing::subscriber::with_default`] or [`tracing::subscriber::set_default`].
///
/// For multi-threaded tests, a global default tracing handler must be used in order to
/// capture messages emitted by all threads. For that use case, acquire the test lock and
/// then use DEFAULT_GLOBAL_TRACING_HANDLER. You can configure the desired verbosity levels
/// for the scope of a test using [`TracingHandler::set_levels`].
///
/// The handler will listen for tracing events for its associated components/levels and
/// publish them to a broadcast channel. To receive the broadcasted events, call
/// [`TracingHandler::subscribe`] to create a new [`TracingSubscriber`].
#[derive(Clone, Debug)]
pub(crate) struct TracingHandler {
    buffer: EventBuffer<TracingEvent>,

    /// Contains a map of (tracing component name, maximum verbosity level) which this handler
    /// will subscribe to messages for. This is stored in an Arc<RwLock> so that we are able
    /// to mutate the global default tracing handler's levels.
    levels: Arc<RwLock<HashMap<String, Level>>>,
}

impl TracingHandler {
    /// Initializes a new tracing handler with no configured components/levels.
    pub(crate) fn new() -> TracingHandler {
        Self::new_with_levels(HashMap::default())
    }

    /// Initializes a new tracing handler with the specified components and corresponding maximum
    /// verbosity levels.
    pub(crate) fn new_with_levels(levels: HashMap<String, Level>) -> TracingHandler {
        Self {
            buffer: EventBuffer::new(),
            levels: Arc::new(RwLock::new(levels)),
        }
    }

    /// Sets the levels for this handler to the provided levels, and returns a
    /// [`TracingLevelsGuard`] which, when dropped, will clear the levels set on this handler.
    /// This can be used to temporarily configure the levels on the global default handler for
    /// the duration of a test.
    pub(crate) fn set_levels(&self, new_levels: HashMap<String, Level>) -> TracingLevelsGuard {
        let mut levels = self.levels.write().unwrap();
        *levels = new_levels;
        TracingLevelsGuard { handler: self }
    }

    /// Returns a `TracingSubscriber` that will listen for tracing events broadcast by this handler.

    pub(crate) fn subscribe(&self) -> EventStream<TracingEvent> {
        self.buffer.stream()
    }
}

/// Convenience type for configuring max verbosity levels on a tracing handler.
/// When dropped the levels for the corresponding handler will be cleared out.
pub(crate) struct TracingLevelsGuard<'a> {
    handler: &'a TracingHandler,
}

impl Drop for TracingLevelsGuard<'_> {
    fn drop(&mut self) {
        self.handler.levels.write().unwrap().clear();
    }
}

/// Merges together the max verbosity levels for components across all test file client entities as
/// well as per-test client entities, so that the a single tracing handler can observe all events
/// any client in the test will expect.
pub(crate) fn max_verbosity_levels_for_test_case(
    entities: &Option<Vec<TestFileEntity>>,
    test_case: &TestCase,
) -> HashMap<String, Level> {
    let mut merged_levels = HashMap::new();

    let mut update_merged_levels = |entity: &TestFileEntity| {
        let client_entity = match entity {
            TestFileEntity::Client(client) => client,
            _ => return,
        };
        if let Some(ref log_levels) = client_entity.observe_log_messages {
            for (component, max_level) in log_levels.iter() {
                match merged_levels.get_mut(component) {
                    Some(current_max) => {
                        *current_max = Ord::max(*current_max, *max_level);
                    }
                    None => {
                        merged_levels.insert(component.clone(), *max_level);
                    }
                }
            }
        }
    };

    // entities that are created in this test via "createEntities" operations
    test_case
        .operations
        .iter()
        .filter(|o| o.name == "createEntities")
        .for_each(|o| {
            o.test_file_entities()
                .unwrap()
                .iter()
                .for_each(&mut update_merged_levels)
        });

    // test-file level entities. these might not all actually be used by this particular
    // test case but we include them for simplicity.
    if let Some(ref entities) = entities {
        entities.iter().for_each(update_merged_levels);
    };

    merged_levels
}

/// Implementation allowing `TracingHandler` to subscribe to `tracing` events.
impl tracing::Subscriber for TracingHandler {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        let levels = self.levels.read().unwrap();
        match levels.get(metadata.target()) {
            Some(level) => metadata.level() <= level,
            None => false,
        }
    }

    /// The default implementation of this method calls `enabled` and returns either
    /// `never` (if `enabled` returns false) or `always` (if`enabled` returns true).
    /// When `always` or `never` is returned, that value is cached for the callsite.
    /// In practice, this prevents us from dynamically changing the interest of the
    /// global tracing handler in particular tracing messages over time, since whatever
    /// value is returned the first time a callsite is hit will always be the value.
    /// By overriding this method to return `sometimes` for all events emitted
    /// by the driver, we tell tracing to dynamically check `enabled` each time the
    /// callsite for the event is hit.
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if metadata.target().starts_with("mongodb") {
            Interest::sometimes()
        } else {
            // at this time, we are only ever interested in tracing events emitted
            // by the driver, so we can safely return `never` for all others.
            Interest::never()
        }
    }

    fn event(&self, event: &tracing::Event<'_>) {
        let mut test_event = TracingEvent::new(
            *event.metadata().level(),
            event.metadata().target().to_string(),
        );
        let mut visitor = TracingEventVisitor::new(&mut test_event);
        event.record(&mut visitor);
        self.buffer.push_event(test_event);
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

    fn record_i128(&mut self, field: &Field, value: i128) {
        self.event
            .fields
            .insert(field.name().to_string(), TracingEventValue::I128(value));
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        self.event
            .fields
            .insert(field.name().to_string(), TracingEventValue::U128(value));
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
