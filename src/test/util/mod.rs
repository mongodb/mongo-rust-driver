mod event;
mod lock;
mod matchable;
mod operation;
mod test_event;
mod test_file;

pub use self::{
    event::{CommandEvent, EventClient},
    lock::TestLock,
    matchable::{assert_matches, Matchable},
    operation::AnyTestOperation,
    test_event::TestEvent,
    test_file::TestFile,
};

use std::{collections::HashMap, fmt::Debug, sync::Arc};

use crate::bson::{doc, oid::ObjectId, Bson};
use semver::Version;
use serde::Deserialize;

use self::event::EventHandler;
use super::CLIENT_OPTIONS;
use crate::{
    error::{CommandError, ErrorKind, Result},
    options::{AuthMechanism, ClientOptions, CreateCollectionOptions},
    Client,
    Collection,
};

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
    pub async fn new() -> Self {
        Self::with_options(None).await
    }

    pub async fn with_options(options: Option<ClientOptions>) -> Self {
        Self::with_handler(None, options).await
    }

    async fn with_handler(
        event_handler: Option<EventHandler>,
        options: impl Into<Option<ClientOptions>>,
    ) -> Self {
        let mut options = options.into().unwrap_or_else(|| CLIENT_OPTIONS.clone());

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
                .await
                .unwrap(),
        ))
        .unwrap();

        let response = client
            .database("test")
            .run_command(doc! { "buildInfo": 1 }, None)
            .await
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

    pub async fn create_user(
        &self,
        user: &str,
        pwd: &str,
        roles: &[Bson],
        mechanisms: &[AuthMechanism],
    ) -> Result<()> {
        let mut cmd = doc! { "createUser": user, "pwd": pwd, "roles": roles };
        if self.server_version_gte(4, 0) {
            let ms: bson::Array = mechanisms.iter().map(|s| Bson::from(s.as_str())).collect();
            cmd.insert("mechanisms", ms);
        }
        self.database("admin").run_command(cmd, None).await?;
        Ok(())
    }

    pub fn get_coll(&self, db_name: &str, coll_name: &str) -> Collection {
        self.database(db_name).collection(coll_name)
    }

    pub async fn init_db_and_coll(&self, db_name: &str, coll_name: &str) -> Collection {
        let coll = self.get_coll(db_name, coll_name);
        drop_collection(&coll).await;
        coll
    }

    pub async fn create_fresh_collection(
        &self,
        db_name: &str,
        coll_name: &str,
        options: impl Into<Option<CreateCollectionOptions>>,
    ) -> Collection {
        self.drop_collection(db_name, coll_name).await;
        self.database(db_name)
            .create_collection(coll_name, options)
            .await
            .unwrap();

        self.get_coll(db_name, coll_name)
    }

    pub fn auth_enabled(&self) -> bool {
        self.options.credential.is_some()
    }

    pub fn is_standalone(&self) -> bool {
        !self.is_replica_set() && !self.is_sharded()
    }

    pub fn is_replica_set(&self) -> bool {
        self.options.repl_set_name.is_some()
    }

    pub fn is_sharded(&self) -> bool {
        self.server_info.msg.as_deref() == Some("isdbgrid")
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

    pub async fn drop_collection(&self, db_name: &str, coll_name: &str) {
        let coll = self.get_coll(db_name, coll_name);
        drop_collection(&coll).await;
    }
}

pub async fn drop_collection(coll: &Collection) {
    match coll.drop(None).await.as_ref().map_err(|e| e.as_ref()) {
        Err(ErrorKind::CommandError(CommandError { code: 26, .. })) | Ok(_) => {}
        e @ Err(_) => {
            e.unwrap();
        }
    };
}

pub fn parse_version(version: &str) -> (u64, u64) {
    let parts: Vec<u64> = version.split('.').map(|s| s.parse().unwrap()).collect();
    if parts.len() != 2 && parts.len() != 3 {
        panic!("not a version string: {}", version);
    }
    (parts[0], parts[1])
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
