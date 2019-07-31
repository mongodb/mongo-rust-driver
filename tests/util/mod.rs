mod event;

pub use self::event::EventClient;

use std::collections::HashMap;

use bson::{oid::ObjectId, Bson};
use mongodb::{
    concern::{Acknowledgment, ReadConcern, WriteConcern},
    error::Result,
    options::{
        auth::{AuthMechanism, Credential},
        ClientOptions,
    },
    read_preference::ReadPreference,
    Client, Collection,
};

use self::event::EventHandler;

static USERNAME: &'static str = "user";
static PASSWORD: &'static str = "pencil";
static MAX_POOL_SIZE: u32 = 100;

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

        if options.repl_set_name.is_some() || options.hosts.len() > 1 {
            options.read_preference = Some(ReadPreference::Primary);
            options.read_concern = Some(ReadConcern::Linearizable);
            options.write_concern =
                Some(WriteConcern::builder().w(Acknowledgment::Majority).build());
        }

        if TestClient::auth_enabled() {
            // Need to create user that this client will authenticate with.
            let cred_creator = Client::with_uri_str(uri).unwrap();
            cred_creator
                .database("admin")
                .run_command(
                    doc! {
                        "createUser": USERNAME, "pwd": PASSWORD, "roles": ["root"]
                    },
                    None,
                )
                .unwrap();

            options.credential = Some(Credential {
                username: Some(USERNAME.to_string()),
                password: Some(PASSWORD.to_string()),
                ..Default::default()
            });
        }

        let client = if let Some(handler) = event_handler {
            Client::with_event_handler(options.clone(), Box::new(handler)).unwrap()
        } else {
            Client::with_options(options.clone()).unwrap()
        };

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

    pub fn auth_enabled() -> bool {
        option_env!("AUTH").unwrap_or("false") == "true"
    }

    pub fn version_at_least_40(&self) -> bool {
        self.server_info
            .max_wire_version
            .map(|v| v >= 7)
            .unwrap_or(false)
    }

    pub fn create_user(
        &self,
        user: &str,
        pwd: &str,
        roles: &[&str],
        mechanisms: &[AuthMechanism],
    ) -> Result<()> {
        let ms: bson::Array = mechanisms.iter().map(|s| Bson::from(s.as_str())).collect();
        let rs: bson::Array = roles.iter().map(|&s| Bson::from(s)).collect();
        let cmd = doc! { "createUser": user, "pwd": pwd, "roles": rs, "mechanisms": ms };
        self.client.database("admin").run_command(cmd, None)?;
        Ok(())
    }

    pub fn get_coll(&self, db_name: &str, coll_name: &str) -> Collection {
        self.database(db_name).collection(coll_name)
    }

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
