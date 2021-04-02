use std::{ops::Deref, sync::Arc};

use crate::{
    bson::Bson,
    test::{CommandEvent, EventHandler},
    Client,
    Collection,
    Database,
};

#[derive(Clone, Debug)]
pub enum Entity {
    Client(ClientEntity),
    Database(Database),
    Collection(Collection),
    Bson(Bson),
    None,
}

#[derive(Clone, Debug)]
pub struct ClientEntity {
    client: Client,
    observer: Arc<EventHandler>,
    pub observe_events: Option<Vec<String>>,
    pub ignore_command_names: Option<Vec<String>>,
}

impl ClientEntity {
    pub fn new(
        client: Client,
        observer: Arc<EventHandler>,
        observe_events: Option<Vec<String>>,
        ignore_command_names: Option<Vec<String>>,
    ) -> Self {
        Self {
            client,
            observer,
            observe_events,
            ignore_command_names,
        }
    }

    /// Gets a list of all of the events of the requested event types that occurred on this client.
    /// Ignores any event with a name in the ignore list. Also ignores all configureFailPoint
    /// events.
    pub fn get_filtered_events(
        &self,
        observe_events: &Option<Vec<String>>,
        ignore_command_names: &Option<Vec<String>>,
    ) -> Vec<CommandEvent> {
        self.observer.get_filtered_command_events(|event| {
            if event.command_name() == "configureFailPoint" {
                return false;
            }
            if let Some(observe_events) = observe_events {
                if !observe_events.iter().any(|name| match event {
                    CommandEvent::CommandStartedEvent(_) => name.as_str() == "commandStartedEvent",
                    CommandEvent::CommandSucceededEvent(_) => {
                        name.as_str() == "commandSucceededEvent"
                    }
                    CommandEvent::CommandFailedEvent(_) => name.as_str() == "commandFailedEvent",
                }) {
                    return false;
                }
            }
            if let Some(ignore_command_names) = ignore_command_names {
                if ignore_command_names
                    .iter()
                    .any(|name| event.command_name() == name)
                {
                    return false;
                }
            }
            true
        })
    }
}

impl From<Database> for Entity {
    fn from(database: Database) -> Self {
        Self::Database(database)
    }
}

impl From<Collection> for Entity {
    fn from(collection: Collection) -> Self {
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

    pub fn as_collection(&self) -> &Collection {
        match self {
            Self::Collection(collection) => collection,
            _ => panic!("Expected collection entity, got {:?}", &self),
        }
    }

    pub fn as_bson(&self) -> &Bson {
        match self {
            Self::Bson(bson) => bson,
            _ => panic!("Expected BSON entity, got {:?}", &self),
        }
    }
}
