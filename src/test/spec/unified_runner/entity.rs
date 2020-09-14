use crate::{bson::Bson, test::util::EventClient, Collection, Database};

pub enum Entity {
    Client(Client),
    Database(Database),
    Collection(Collection),
    Result(Bson),
    None,
}

pub struct Client {
    client: EventClient,
    observe_events: Option<Vec<String>>,
    ignore_events: Option<Vec<String>>,
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
    pub fn from_client(
        client: EventClient,
        observe_events: Option<Vec<String>>,
        ignore_events: Option<Vec<String>>,
    ) -> Self {
        Self::Client(Client {
            client,
            observe_events,
            ignore_events,
        })
    }

    pub fn as_client(&self) -> &EventClient {
        match self {
            Self::Client(client) => &client.client,
            _ => panic!("Entity not a client"),
        }
    }

    pub fn get_observe_events(&self) -> &Option<Vec<String>> {
        match self {
            Self::Client(client) => &client.observe_events,
            _ => panic!("Entity not a client"),
        }
    }

    pub fn get_ignore_events(&self) -> &Option<Vec<String>> {
        match self {
            Self::Client(client) => &client.ignore_events,
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
