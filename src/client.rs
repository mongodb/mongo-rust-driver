use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, RwLock,
    },
};

use crate::{
    concern::{ReadConcern, WriteConcern},
    db::Database,
    error::Result,
    event::{
        CommandEventHandler, CommandFailedEvent, CommandStartedEvent, CommandSucceededEvent,
        HandlerId,
    },
    options::DatabaseOptions,
    read_preference::ReadPreference,
};

/// This is the main entry point for the API. A `Client` is used to connect to a MongoDB cluster.
/// By default, it will monitor the topology of the cluster, keeping track of any changes, such
/// as servers being added or removed
///
/// `Client` uses [`std::sync::Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html) internally,
/// so it can safely be shared across threads. For example:
///
/// ```rust
/// 
/// # use mongodb::{Client, error::Result};
///
/// # fn start_workers() -> Result<()> {
/// let client = Client::with_uri("mongodb://example.com")?;
///
/// for i in 0..5 {
///     let client_ref = client.clone();
///
///     std::thread::spawn(move || {
///         let collection = client_ref.database("items").collection(&format!("coll{}", i));
///
///         // Do something with the collection
///     });
/// }
/// #
/// # // Technically we should join the threads here, but for the purpose of the example, we'll just
/// # // sleep for a bit.
/// # std::thread::sleep(std::time::Duration::from_secs(3));
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<ClientInner>,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct ClientInner {
    read_preference: Option<ReadPreference>,
    read_concern: Option<ReadConcern>,
    write_concern: Option<WriteConcern>,
    next_handler_id: AtomicU32,
    #[derivative(Debug = "ignore")]
    command_event_handlers: RwLock<HashMap<HandlerId, Box<CommandEventHandler>>>,
}

impl Client {
    /// Creates a new `Client` connected to the cluster specified by `uri`. `uri` must be a valid
    /// MongoDB connection string.
    pub fn with_uri(uri: &str) -> Result<Self> {
        unimplemented!()
    }

    /// Gets the read preference of the `Client`.
    pub fn read_preference(&self) -> Option<&ReadPreference> {
        unimplemented!()
    }

    /// Gets the read concern of the `Client`.
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        unimplemented!()
    }

    /// Gets the write concern of the `Client`.
    pub fn write_concern(&self) -> Option<&WriteConcern> {
        unimplemented!()
    }

    /// Gets a handle to a database specified by `name` in the cluster the `Client` is connected to.
    /// The `Database` options (e.g. read preference and write concern) will default to those of the
    /// `Client`.
    ///
    /// This method does not send or receive anything across the wire to the database, so it can be
    /// used repeatedly without incurring any costs from I/O.
    pub fn database(&self, name: &str) -> Database {
        unimplemented!()
    }

    /// Gets a handle to a database specified by `name` in the cluster the `Client` is connected to.
    /// Operations done with this `Database` will use the options specified by `options` by default
    /// and will otherwise default to those of the `Client`.
    ///
    /// This method does not send or receive anything across the wire to the database, so it can be
    /// used repeatedly without incurring any costs from I/O.
    pub fn database_with_options(&self, name: &str, options: DatabaseOptions) -> Database {
        unimplemented!()
    }

    /// Registers an event handler to receive notifications whenever a command-related event occurs.
    pub fn add_command_event_handler(&self, handler: Box<dyn CommandEventHandler>) -> HandlerId {
        let id = HandlerId::new(self.inner.next_handler_id.fetch_add(1, Ordering::SeqCst));
        self.inner
            .command_event_handlers
            .write()
            .unwrap()
            .insert(id, handler);

        id
    }

    /// Unregisters an event handler so that it no longer receives notifications when an event
    /// occurs.
    pub fn remove_event_handler(&self, id: HandlerId) {
        unimplemented!()
    }

    pub(crate) fn send_command_started_event(&self, event: CommandStartedEvent) {
        for handler in self
            .inner
            .command_event_handlers
            .write()
            .unwrap()
            .values_mut()
        {
            handler.handle_command_started_event(event.clone());
        }
    }

    pub(crate) fn send_command_succeeded_event(&self, event: CommandSucceededEvent) {
        for handler in self
            .inner
            .command_event_handlers
            .write()
            .unwrap()
            .values_mut()
        {
            handler.handle_command_succeeded_event(event.clone());
        }
    }

    pub(crate) fn send_command_failed_event(&self, event: CommandFailedEvent) {
        for handler in self
            .inner
            .command_event_handlers
            .write()
            .unwrap()
            .values_mut()
        {
            handler.handle_command_failed_event(event.clone());
        }
    }
}
