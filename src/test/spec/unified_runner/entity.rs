use std::ops::Deref;

use crate::{bson::Bson, test::util::EventClient, Collection, Database};

pub enum Entity {
    Client(ClientEntity),
    Database(Database),
    Collection(Collection),
    Result(Bson),
    None,
}

pub struct ClientEntity {
    client: EventClient,
    pub observe_events: Option<Vec<String>>,
    pub ignore_command_names: Option<Vec<String>>,
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

impl From<Option<Bson>> for Entity {
    fn from(result: Option<Bson>) -> Self {
        match result {
            Some(result) => Self::Result(result),
            None => Self::None,
        }
    }
}

impl Deref for ClientEntity {
    type Target = EventClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl Entity {
    pub fn from_client(
        client: EventClient,
        observe_events: Option<Vec<String>>,
        ignore_command_names: Option<Vec<String>>,
    ) -> Self {
        Self::Client(ClientEntity {
            client,
            observe_events,
            ignore_command_names,
        })
    }

    pub fn as_client(&self) -> &ClientEntity {
        match self {
            Self::Client(client) => client,
            _ => panic!("Entity not a client"),
        }
    }

    pub fn as_database(&self) -> &Database {
        match self {
            Self::Database(database) => database,
            _ => panic!("Entity not a database"),
        }
    }
}
