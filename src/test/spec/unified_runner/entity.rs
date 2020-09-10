use crate::{bson::Bson, test::util::EventClient, Collection, Database};

pub enum Entity {
    Client(EventClient),
    Database(Database),
    Collection(Collection),
    Result(Bson),
    None,
}

impl From<EventClient> for Entity {
    fn from(client: EventClient) -> Self {
        Self::Client(client)
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

impl From<Option<Bson>> for Entity {
    fn from(result: Option<Bson>) -> Self {
        match result {
            Some(result) => Self::Result(result),
            None => Self::None,
        }
    }
}

impl Entity {
    pub fn as_client(&self) -> &EventClient {
        match self {
            Self::Client(event_client) => event_client,
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
