mod event;
mod lock;

pub use self::{event::EventClient, lock::TestLock};

use std::{collections::HashMap, sync::Arc};

use bson::{bson, doc, oid::ObjectId, Bson};
use semver::Version;
use serde::Deserialize;

use self::event::EventHandler;
use crate::{
    error::{CommandError, ErrorKind, Result},
    options::{auth::AuthMechanism, ClientOptions},
    Client,
    Collection,
};

const MAX_POOL_SIZE: u32 = 100;

pub struct TestClient {
    client: Client,
    pub options: ClientOptions,
    pub server_info: IsMasterCommandResponse,
    pub server_version: Version,
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
            let handler = Arc::new(event_handler);
            options.command_event_handler = Some(handler.clone());
            options.cmap_event_handler = Some(handler);
        }

        let client = Client::with_options(options.clone()).unwrap();

        let server_info = bson::from_bson(Bson::Document(
            client
                .database("admin")
                .run_command(doc! { "isMaster":  1 }, None)
                .unwrap(),
        ))
        .unwrap();

        let response = client
            .database("test")
            .run_command(doc! { "buildInfo": 1 }, None)
            .unwrap();

        let info: BuildInfo = bson::from_bson(Bson::Document(response)).unwrap();
        let server_version = Version::parse(&info.version).unwrap();

        Self {
            client,
            options,
            server_info,
            server_version,
        }
    }

    pub fn create_user(
        &self,
        user: &str,
        pwd: &str,
        roles: &[&str],
        mechanisms: &[AuthMechanism],
    ) -> Result<()> {
        let rs: bson::Array = roles.iter().map(|&s| Bson::from(s)).collect();
        let mut cmd = doc! { "createUser": user, "pwd": pwd, "roles": rs };
        if self.server_version_gte(4, 0) {
            let ms: bson::Array = mechanisms.iter().map(|s| Bson::from(s.as_str())).collect();
            cmd.insert("mechanisms", ms);
        }
        self.database("admin").run_command(cmd, None)?;
        Ok(())
    }

    pub fn get_coll(&self, db_name: &str, coll_name: &str) -> Collection {
        self.database(db_name).collection(coll_name)
    }

    pub fn init_db_and_coll(&self, db_name: &str, coll_name: &str) -> Collection {
        let coll = self.get_coll(db_name, coll_name);
        drop_collection(&coll);
        coll
    }

    pub fn auth_enabled(&self) -> bool {
        self.options.credential.is_some()
    }

    #[allow(dead_code)]
    pub fn server_version_eq(&self, major: u64, minor: u64) -> bool {
        self.server_version.major == major && self.server_version.minor == minor
    }

    #[allow(dead_code)]
    pub fn server_version_gt(&self, major: u64, minor: u64) -> bool {
        self.server_version.major > major
            || (self.server_version.major == major && self.server_version.minor > minor)
    }

    #[allow(dead_code)]
    pub fn server_version_gte(&self, major: u64, minor: u64) -> bool {
        self.server_version.major > major
            || (self.server_version.major == major && self.server_version.minor >= minor)
    }

    pub fn server_version_lt(&self, major: u64, minor: u64) -> bool {
        self.server_version.major < major
            || (self.server_version.major == major && self.server_version.minor < minor)
    }

    #[allow(dead_code)]
    pub fn server_version_lte(&self, major: u64, minor: u64) -> bool {
        self.server_version.major < major
            || (self.server_version.major == major && self.server_version.minor <= minor)
    }

    pub fn drop_collection(&self, db_name: &str, coll_name: &str) {
        let coll = self.get_coll(db_name, coll_name);
        drop_collection(&coll);
    }
}

pub fn drop_collection(coll: &Collection) {
    match coll.drop(None).as_ref().map_err(|e| e.as_ref()) {
        Err(ErrorKind::CommandError(CommandError { code: 26, .. })) | Ok(_) => {}
        e @ Err(_) => {
            e.unwrap();
        }
    };
}

#[derive(Debug, Deserialize)]
struct BuildInfo {
    version: String,
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
