mod event;

pub use self::event::EventClient;

use std::{collections::HashMap, sync::Arc};

use bson::{bson, doc, oid::ObjectId, Bson};
use mongodb::{options::ClientOptions, Client, Collection};
use serde::Deserialize;

use self::event::EventHandler;

const MAX_POOL_SIZE: u32 = 100;

pub struct TestClient {
    client: Client,
    pub options: ClientOptions,
    pub server_info: IsMasterCommandResponse,
}

impl std::ops::Deref for TestClient {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl TestClient {
    pub fn new() -> Self {
        Self::with_handler(None)
    }

    fn with_handler(event_handler: Option<EventHandler>) -> Self {
        let uri = option_env!("MONGODB_URI").unwrap_or("mongodb://localhost:27017");
        let mut options = ClientOptions::parse(uri).unwrap();
        options.max_pool_size = Some(MAX_POOL_SIZE);

        if let Some(event_handler) = event_handler {
            options.command_event_handler = Some(Arc::new(event_handler));
        }

        let client = Client::with_options(options.clone()).unwrap();

        let server_info = bson::from_bson(Bson::Document(
            client
                .database("admin")
                .run_command(doc! { "isMaster":  1 }, None)
                .unwrap(),
        ))
        .unwrap();

        Self {
            client,
            options,
            server_info,
        }
    }

    #[allow(dead_code)]
    pub fn get_coll(&self, db_name: &str, coll_name: &str) -> Collection {
        self.database(db_name).collection(coll_name)
    }

    #[allow(dead_code)]
    pub fn init_db_and_coll(&self, db_name: &str, coll_name: &str) -> Collection {
        let coll = self.get_coll(db_name, coll_name);
        coll.drop().unwrap();
        coll
    }
}

// Copy of the internal isMaster struct; fix this later.
#[derive(Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IsMasterCommandResponse {
    #[serde(rename = "ismaster")]
    pub is_master: Option<bool>,
    pub ok: Option<f32>,
    pub hosts: Option<Vec<String>>,
    pub passives: Option<Vec<String>>,
    pub arbiters: Option<Vec<String>>,
    pub msg: Option<String>,
    pub me: Option<String>,
    pub set_version: Option<i32>,
    pub set_name: Option<String>,
    pub hidden: Option<bool>,
    pub secondary: Option<bool>,
    pub arbiter_only: Option<bool>,
    #[serde(rename = "isreplicaset")]
    pub is_replica_set: Option<bool>,
    pub logical_session_timeout_minutes: Option<i64>,
    pub min_wire_version: Option<i32>,
    pub max_wire_version: Option<i32>,
    pub tags: Option<HashMap<String, String>>,
    pub election_id: Option<ObjectId>,
    pub primary: Option<String>,
}
