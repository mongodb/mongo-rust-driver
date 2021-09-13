use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use crate::{
    bson::{Bson, Document},
    client::{HELLO_COMMAND_NAMES, REDACTED_COMMANDS},
    event::command::CommandStartedEvent,
    test::{CommandEvent, EventHandler, spec::unified_runner::ObserveEvent},
    Client,
    ClientSession,
    Collection,
    Database,
};

#[derive(Debug)]
pub enum Entity {
    Client(ClientEntity),
    Database(Database),
    Collection(Collection<Document>),
    Session(SessionEntity),
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
    pub fn get_filtered_events(&self) -> Vec<CommandEvent> {
        self.observer.get_filtered_command_events(|event| {
            if event.command_name() == "configureFailPoint" {
                return false;
            }
            if let Some(observe_events) = self.observe_events.as_ref() {
                if !observe_events.iter().any(|observe| observe.matches(event)) {
                    return false;
                }
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
        })
    }

    /// Gets all events of type commandStartedEvent, excluding configureFailPoint events.
    pub fn get_all_command_started_events(&self) -> Vec<CommandStartedEvent> {
        self.observer.get_all_command_started_events()
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
}
