use std::{fmt, ops::Deref};

use crate::{bson::Bson, test::util::EventClient, Collection, Database};

#[derive(Clone)]
pub enum Entity {
    Client(ClientEntity),
    Database(Database),
    Collection(Collection),
    Bson(Bson),
    None,
}

impl fmt::Debug for Entity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client(_) => f.write_str("client"),
            Self::Database(_) => f.write_str("database"),
            Self::Collection(_) => f.write_str("collection"),
            Self::Bson(_) => f.write_str("bson"),
            Self::None => f.write_str("none"),
        }
    }
}

#[derive(Clone)]
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

impl From<Bson> for Entity {
    fn from(bson: Bson) -> Self {
        Self::Bson(bson)
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

    pub fn as_collection(&self) -> &Collection {
        match self {
            Self::Collection(collection) => collection,
            _ => panic!("Entity not a collection"),
        }
    }
}
