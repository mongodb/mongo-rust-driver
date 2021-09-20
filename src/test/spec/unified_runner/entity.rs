use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use tokio::sync::{oneshot, Mutex};

use crate::{
    bson::{Bson, Document},
    client::{HELLO_COMMAND_NAMES, REDACTED_COMMANDS},
    event::command::CommandStartedEvent,
    test::{
        spec::unified_runner::{ExpectedEventType, ObserveEvent},
        CommandEvent,
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

#[derive(Debug)]
pub enum Entity {
    Client(ClientEntity),
    Database(Database),
    Collection(Collection<Document>),
    Session(SessionEntity),
    FindCursor(FindCursor),
    Bson(Bson),
    None,
}

#[derive(Clone, Debug)]
pub struct ClientEntity {
    client: Client,
    observer: Arc<EventHandler>,
    observe_events: Option<Vec<ObserveEvent>>,
    ignore_command_names: Option<Vec<String>>,
    observe_sensitive_commands: bool,
}

#[derive(Debug)]
pub struct SessionEntity {
    pub lsid: Document,
    pub client_session: Option<Box<ClientSession>>,
}

#[derive(Debug)]
pub enum FindCursor {
    // Due to https://github.com/rust-lang/rust/issues/59245, the `Entity` type is required to be
    // `Sync`; however, `Cursor` is `!Sync` due to internally storing a `BoxFuture`, which only
    // has a `Send` bound.  Wrapping it in `Mutex` works around this.
    Normal(Mutex<Cursor<Document>>),
    Session {
        cursor: SessionCursor<Document>,
        session_id: String,
    },
    Closed,
}

impl FindCursor {
    pub async fn make_kill_watcher(&mut self) -> oneshot::Receiver<()> {
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
            Self::Closed => panic!("cannot set a kill_watcher on a closed cursor"),
        }
    }
}

impl ClientEntity {
    pub fn new(
        client: Client,
        observer: Arc<EventHandler>,
        observe_events: Option<Vec<ObserveEvent>>,
        ignore_command_names: Option<Vec<String>>,
        observe_sensitive_commands: bool,
    ) -> Self {
        Self {
            client,
            observer,
            observe_events,
            ignore_command_names,
            observe_sensitive_commands,
        }
    }

    /// Gets a list of all of the events of the requested event types that occurred on this client.
    /// Ignores any event with a name in the ignore list. Also ignores all configureFailPoint
    /// events.
    pub fn get_filtered_events(&self, expected_type: ExpectedEventType) -> Vec<Event> {
        self.observer.get_filtered_events(expected_type, |event| {
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
    pub fn get_all_command_started_events(&self) -> Vec<CommandStartedEvent> {
        self.observer.get_all_command_started_events()
    }

    /// Gets the count of connections currently checked out.
    pub fn connections_checked_out(&self) -> u32 {
        self.observer.connections_checked_out()
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

impl Deref for ClientEntity {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl SessionEntity {
    pub fn new(client_session: ClientSession) -> Self {
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
    pub fn as_client(&self) -> &ClientEntity {
        match self {
            Self::Client(client) => client,
            _ => panic!("Expected client entity, got {:?}", &self),
        }
    }

    pub fn as_database(&self) -> &Database {
        match self {
            Self::Database(database) => database,
            _ => panic!("Expected database entity, got {:?}", &self),
        }
    }

    pub fn as_collection(&self) -> &Collection<Document> {
        match self {
            Self::Collection(collection) => collection,
            _ => panic!("Expected collection entity, got {:?}", &self),
        }
    }

    pub fn as_session_entity(&self) -> &SessionEntity {
        match self {
            Self::Session(client_session) => client_session,
            _ => panic!("Expected client session entity, got {:?}", &self),
        }
    }

    pub fn as_mut_session_entity(&mut self) -> &mut SessionEntity {
        match self {
            Self::Session(client_session) => client_session,
            _ => panic!("Expected mutable client session entity, got {:?}", &self),
        }
    }

    pub fn as_bson(&self) -> &Bson {
        match self {
            Self::Bson(bson) => bson,
            _ => panic!("Expected BSON entity, got {:?}", &self),
        }
    }

    pub fn as_mut_find_cursor(&mut self) -> &mut FindCursor {
        match self {
            Self::FindCursor(cursor) => cursor,
            _ => panic!("Expected find cursor, got {:?}", &self),
        }
    }

    pub fn into_find_cursor(self) -> FindCursor {
        match self {
            Self::FindCursor(cursor) => cursor,
            _ => panic!("Expected find cursor, got {:?}", &self),
        }
    }
}
