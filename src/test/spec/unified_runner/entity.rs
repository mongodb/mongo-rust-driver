use std::{
    fs::File,
    io::BufWriter,
    ops::{Deref, DerefMut},
    sync::Arc,
    time::Duration,
};

use tokio::sync::{mpsc, oneshot, Mutex};

use crate::{
    bson::{Bson, Document},
    change_stream::ChangeStream,
    client::{HELLO_COMMAND_NAMES, REDACTED_COMMANDS},
    error::{Error, Result},
    event::command::{CommandEvent, CommandStartedEvent},
    gridfs::GridFsBucket,
    runtime,
    sdam::TopologyDescription,
    test::{
        spec::unified_runner::{ExpectedEventType, ObserveEvent},
        Event,
        EventHandler,
    },
    Client,
    ClientSession,
    Collection,
    Cursor,
    Database,
    SessionCursor,
};

use super::{observer::EventObserver, test_file::ThreadMessage, Operation};

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum Entity {
    Client(ClientEntity),
    Database(Database),
    Collection(Collection<Document>),
    Session(SessionEntity),
    Bucket(GridFsBucket),
    Cursor(TestCursor),
    Bson(Bson),
    EventList(EventList),
    Thread(ThreadEntity),
    TopologyDescription(TopologyDescription),
    None,
}

#[derive(Clone, Debug)]
pub(crate) struct ClientEntity {
    client: Client,
    handler: Arc<EventHandler>,
    pub(crate) observer: Arc<Mutex<EventObserver>>,
    observe_events: Option<Vec<ObserveEvent>>,
    ignore_command_names: Option<Vec<String>>,
    observe_sensitive_commands: bool,
}

#[derive(Debug)]
pub(crate) struct SessionEntity {
    pub(crate) lsid: Document,
    pub(crate) client_session: Option<Box<ClientSession>>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(crate) enum TestCursor {
    // Due to https://github.com/rust-lang/rust/issues/59245, the `Entity` type is required to be
    // `Sync`; however, `Cursor` is `!Sync` due to internally storing a `BoxFuture`, which only
    // has a `Send` bound.  Wrapping it in `Mutex` works around this.
    Normal(Mutex<Cursor<Document>>),
    Session {
        cursor: SessionCursor<Document>,
        session_id: String,
    },
    // `ChangeStream` has the same issue with 59245 as `Cursor`.
    ChangeStream(Mutex<ChangeStream<Document>>),
    Closed,
}

#[derive(Debug)]
pub struct EventList {
    pub client_id: String,
    pub event_names: Vec<String>,
}

impl From<EventList> for Entity {
    fn from(event_list: EventList) -> Self {
        Self::EventList(event_list)
    }
}

impl TestCursor {
    pub(crate) async fn make_kill_watcher(&mut self) -> oneshot::Receiver<()> {
        match self {
            Self::Normal(cursor) => {
                let (tx, rx) = oneshot::channel();
                cursor.lock().await.set_kill_watcher(tx);
                rx
            }
            Self::Session { cursor, .. } => {
                let (tx, rx) = oneshot::channel();
                cursor.set_kill_watcher(tx);
                rx
            }
            Self::ChangeStream(stream) => {
                let (tx, rx) = oneshot::channel();
                stream.lock().await.set_kill_watcher(tx);
                rx
            }
            Self::Closed => panic!("cannot set a kill_watcher on a closed cursor"),
        }
    }
}

impl ClientEntity {
    pub(crate) fn new(
        client: Client,
        handler: Arc<EventHandler>,
        observe_events: Option<Vec<ObserveEvent>>,
        ignore_command_names: Option<Vec<String>>,
        observe_sensitive_commands: bool,
    ) -> Self {
        let observer = EventObserver::new(handler.broadcaster().subscribe());
        Self {
            client,
            handler,
            observer: Arc::new(Mutex::new(observer)),
            observe_events,
            ignore_command_names,
            observe_sensitive_commands,
        }
    }

    /// Gets a list of all of the events of the requested event types that occurred on this client.
    /// Ignores any event with a name in the ignore list. Also ignores all configureFailPoint
    /// events.
    pub(crate) fn get_filtered_events(&self, expected_type: ExpectedEventType) -> Vec<Event> {
        self.handler.get_filtered_events(expected_type, |event| {
            if let Event::Command(cev) = event {
                if !self.allow_command_event(cev) {
                    return false;
                }
            }
            if let Some(observe_events) = self.observe_events.as_ref() {
                if !observe_events.iter().any(|observe| observe.matches(event)) {
                    return false;
                }
            }
            true
        })
    }

    /// Returns `true` if a given `CommandEvent` is allowed to be observed.
    fn allow_command_event(&self, event: &CommandEvent) -> bool {
        if event.command_name() == "configureFailPoint" {
            return false;
        }
        if let Some(ignore_command_names) = self.ignore_command_names.as_ref() {
            if ignore_command_names
                .iter()
                .any(|name| event.command_name().eq_ignore_ascii_case(name))
            {
                return false;
            }
        }
        if !self.observe_sensitive_commands {
            let lower_name = event.command_name().to_ascii_lowercase();
            // If a hello command has been redacted, it's sensitive and the event should be
            // ignored.
            let is_sensitive_hello = HELLO_COMMAND_NAMES.contains(lower_name.as_str())
                && match event {
                    CommandEvent::Started(ev) => ev.command.is_empty(),
                    CommandEvent::Succeeded(ev) => ev.reply.is_empty(),
                    CommandEvent::Failed(_) => false,
                };
            if is_sensitive_hello || REDACTED_COMMANDS.contains(lower_name.as_str()) {
                return false;
            }
        }
        true
    }

    /// Gets all events of type commandStartedEvent, excluding configureFailPoint events.
    pub(crate) fn get_all_command_started_events(&self) -> Vec<CommandStartedEvent> {
        self.handler.get_all_command_started_events()
    }

    /// Writes all events with the given name to the given BufWriter.
    pub fn write_events_list_to_file(&self, names: &[&str], writer: &mut BufWriter<File>) {
        self.handler.write_events_list_to_file(names, writer);
    }

    /// Gets the count of connections currently checked out.
    pub(crate) fn connections_checked_out(&self) -> u32 {
        self.handler.connections_checked_out()
    }

    /// Synchronize all connection pool worker threads.
    pub(crate) async fn sync_workers(&self) {
        self.client.sync_workers().await;
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ThreadEntity {
    pub(crate) sender: mpsc::UnboundedSender<ThreadMessage>,
}

impl ThreadEntity {
    pub(crate) fn run_operation(&self, op: Arc<Operation>) {
        self.sender
            .send(ThreadMessage::ExecuteOperation(op))
            .unwrap();
    }

    pub(crate) async fn wait(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        // if the task panicked, this send will fail
        if self.sender.send(ThreadMessage::Stop(tx)).is_err() {
            return Err(Error::internal("thread stopped before it was waited for"));
        }

        // return that both the timeout was satisfied and that the task responded to the
        // acknowledgment request.
        runtime::timeout(Duration::from_secs(10), rx)
            .await
            .map_err(|_| Error::internal("timeout waiting for thread to stop"))
            .and_then(|r| r.map_err(|_| Error::internal("thread stopped before it was waited for")))
    }
}

impl From<Database> for Entity {
    fn from(database: Database) -> Self {
        Self::Database(database)
    }
}

impl From<Collection<Document>> for Entity {
    fn from(collection: Collection<Document>) -> Self {
        Self::Collection(collection)
    }
}

impl From<Bson> for Entity {
    fn from(bson: Bson) -> Self {
        Self::Bson(bson)
    }
}

impl From<TopologyDescription> for Entity {
    fn from(td: TopologyDescription) -> Self {
        Self::TopologyDescription(td)
    }
}

impl From<GridFsBucket> for Entity {
    fn from(bucket: GridFsBucket) -> Self {
        Self::Bucket(bucket)
    }
}

impl Deref for ClientEntity {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl SessionEntity {
    pub(crate) fn new(client_session: ClientSession) -> Self {
        let lsid = client_session.id().clone();
        Self {
            client_session: Some(Box::new(client_session)),
            lsid,
        }
    }
}

impl Deref for SessionEntity {
    type Target = ClientSession;
    fn deref(&self) -> &Self::Target {
        self.client_session
            .as_ref()
            .unwrap_or_else(|| panic!("Tried to access dropped client session from entity map"))
    }
}

impl DerefMut for SessionEntity {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client_session
            .as_mut()
            .unwrap_or_else(|| panic!("Tried to access dropped client session from entity map"))
    }
}

impl Entity {
    pub(crate) fn as_client(&self) -> &ClientEntity {
        match self {
            Self::Client(client) => client,
            _ => panic!("Expected client entity, got {:?}", &self),
        }
    }

    pub(crate) fn as_database(&self) -> &Database {
        match self {
            Self::Database(database) => database,
            _ => panic!("Expected database entity, got {:?}", &self),
        }
    }

    pub(crate) fn as_collection(&self) -> &Collection<Document> {
        match self {
            Self::Collection(collection) => collection,
            _ => panic!("Expected collection entity, got {:?}", &self),
        }
    }

    pub(crate) fn as_session_entity(&self) -> &SessionEntity {
        match self {
            Self::Session(client_session) => client_session,
            _ => panic!("Expected client session entity, got {:?}", &self),
        }
    }

    pub fn as_bucket_entity(&self) -> &GridFsBucket {
        match self {
            Self::Bucket(gridfs_bucket) => gridfs_bucket,
            _ => panic!("Expected bucket entity, got {:?}", &self),
        }
    }

    pub fn as_mut_session_entity(&mut self) -> &mut SessionEntity {
        match self {
            Self::Session(client_session) => client_session,
            _ => panic!("Expected mutable client session entity, got {:?}", &self),
        }
    }

    pub(crate) fn as_bson(&self) -> &Bson {
        match self {
            Self::Bson(bson) => bson,
            _ => panic!("Expected BSON entity, got {:?}", &self),
        }
    }

    pub(crate) fn as_mut_cursor(&mut self) -> &mut TestCursor {
        match self {
            Self::Cursor(cursor) => cursor,
            _ => panic!("Expected cursor, got {:?}", &self),
        }
    }

    pub(crate) fn as_thread(&self) -> &ThreadEntity {
        match self {
            Self::Thread(thread) => thread,
            _ => panic!("Expected thread, got {:?}", self),
        }
    }

    pub(crate) fn as_topology_description(&self) -> &TopologyDescription {
        match self {
            Self::TopologyDescription(desc) => desc,
            _ => panic!("Expected Topologydescription, got {:?}", self),
        }
    }

    pub(crate) fn into_cursor(self) -> TestCursor {
        match self {
            Self::Cursor(cursor) => cursor,
            _ => panic!("Expected cursor, got {:?}", &self),
        }
    }

    pub fn as_event_list(&self) -> &EventList {
        match self {
            Self::EventList(event_list) => event_list,
            _ => panic!("Expected event list, got {:?}", &self),
        }
    }
}
