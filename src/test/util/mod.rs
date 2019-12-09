mod event;
mod lock;

pub use self::{
    event::{CommandEvent, EventClient},
    lock::TestLock,
};

use std::{any::Any, collections::HashMap, fmt::Debug, sync::Arc};

use bson::{bson, doc, oid::ObjectId, Bson, Document};
use semver::Version;
use serde::Deserialize;

use self::event::EventHandler;
use crate::{
    bson_util,
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

pub fn parse_version(version: &str) -> (u64, u64) {
    let parts: Vec<u64> = version.split('.').map(|s| s.parse().unwrap()).collect();
    if parts.len() != 2 {
        panic!("not two part version string: {:?}", parts);
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

pub trait Matchable: Sized + 'static {
    fn is_placeholder(&self) -> bool {
        false
    }

    fn content_matches(&self, expected: &Self) -> bool;

    fn matches<T: Matchable + Any>(&self, expected: &T) -> bool {
        if expected.is_placeholder() {
            return true;
        }
        if let Some(expected) = Any::downcast_ref::<Self>(expected) {
            self.content_matches(expected)
        } else {
            false
        }
    }
}

impl Matchable for Bson {
    fn is_placeholder(&self) -> bool {
        if let Bson::String(string) = self {
            string.as_str() == "42" || string.as_str() == ""
        } else {
            get_int(self) == Some(42)
        }
    }

    fn content_matches(&self, expected: &Bson) -> bool {
        match (self, expected) {
            (Bson::Document(actual_doc), Bson::Document(expected_doc)) => {
                actual_doc.matches(expected_doc)
            }
            (Bson::Array(actual_array), Bson::Array(expected_array)) => {
                for (i, expected_element) in expected_array.iter().enumerate() {
                    if actual_array.len() <= i || !actual_array[i].matches(expected_element) {
                        return false;
                    }
                }
                true
            }
            _ => match (bson_util::get_int(self), get_int(expected)) {
                (Some(actual_int), Some(expected_int)) => actual_int == expected_int,
                (None, Some(_)) => false,
                _ => self == expected,
            },
        }
    }
}

impl Matchable for Document {
    fn content_matches(&self, expected: &Document) -> bool {
        for (k, v) in expected.iter() {
            if let Some(actual_v) = self.get(k) {
                if !actual_v.matches(v) {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }
}

pub fn assert_matches<A: Matchable + Debug, E: Matchable + Debug>(actual: &A, expected: &E) {
    assert!(
        actual.matches(expected),
        "\n{:?}\n did not MATCH \n{:?}",
        actual,
        expected
    );
}

fn parse_i64_ext_json(doc: &Document) -> Option<i64> {
    let number_string = doc.get("$numberLong").and_then(Bson::as_str)?;
    number_string.parse::<i64>().ok()
}

fn get_int(value: &Bson) -> Option<i64> {
    bson_util::get_int(value).or_else(|| value.as_document().and_then(parse_i64_ext_json))
}
