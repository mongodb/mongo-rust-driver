use std::ops::Deref;

use crate::{bson::Bson, test::util::EventClient, Collection, Database};

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
