mod event;
mod failpoint;
mod lock;
mod matchable;

pub use self::{
    event::{CmapEvent, CommandEvent, Event, EventClient, EventHandler, SdamEvent},
    failpoint::{FailCommandOptions, FailPoint, FailPointGuard, FailPointMode},
    lock::TestLock,
    matchable::{assert_matches, eq_matches, MatchErrExt, Matchable},
};

use std::{fmt::Debug, sync::Arc, time::Duration};

use crate::{
    bson::{doc, Bson},
    hello::{hello_command, HelloCommandResponse},
    selection_criteria::SelectionCriteria,
};
use bson::Document;
use semver::{Version, VersionReq};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::CLIENT_OPTIONS;
use crate::{
    error::{CommandError, ErrorKind, Result},
    operation::RunCommand,
    options::{AuthMechanism, ClientOptions, CollectionOptions, CreateCollectionOptions},
    test::{
        update_options_for_testing,
        Topology,
        LOAD_BALANCED_MULTIPLE_URI,
        LOAD_BALANCED_SINGLE_URI,
        SERVERLESS,
    },
    Client,
    Collection,
};

#[derive(Clone, Debug)]
pub struct TestClient {
    client: Client,
    pub options: ClientOptions,
    pub(crate) server_info: HelloCommandResponse,
    pub server_version: Version,
    pub server_parameters: Document,
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

    pub async fn with_handler(
        event_handler: Option<Arc<EventHandler>>,
        options: impl Into<Option<ClientOptions>>,
    ) -> Self {
        let mut options = match options.into() {
            Some(options) => options,
            None => CLIENT_OPTIONS.get().await.clone(),
        };

        if let Some(handler) = event_handler {
            options.command_event_handler = Some(handler.clone());
            options.cmap_event_handler = Some(handler.clone());
            options.sdam_event_handler = Some(handler);
        }

        let client = Client::with_options(options.clone()).unwrap();

        // To avoid populating the session pool with leftover implicit sessions, we check out a
        // session here and immediately mark it as dirty, then use it with any operations we need.
        let mut session = client
            .start_session_with_timeout(Some(Duration::from_secs(60 * 60)), None, true)
            .await;
        session.mark_dirty();

        let hello_cmd = hello_command(options.server_api.as_ref(), options.load_balanced, None);
        let hello = RunCommand::new("admin".into(), hello_cmd.body, None, None).unwrap();

        let server_info = bson::from_bson(Bson::Document(
            client.execute_operation(hello, &mut session).await.unwrap(),
        ))
        .unwrap();

        let build_info =
            RunCommand::new("test".into(), doc! { "buildInfo":  1 }, None, None).unwrap();

        let response = client
            .execute_operation(build_info, &mut session)
            .await
            .unwrap();

        let info: BuildInfo = bson::from_bson(Bson::Document(response)).unwrap();
        let server_version_str = info.version.split('-').next().unwrap();
        let server_version = Version::parse(server_version_str).unwrap();

        let get_parameters =
            RunCommand::new("admin".into(), doc! { "getParameter": "*" }, None, None).unwrap();
        let server_parameters = client
            .execute_operation(get_parameters, &mut session)
            .await
            .unwrap_or_default();

        Self {
            client,
            options,
            server_info,
            server_version,
            server_parameters,
        }
    }

    pub async fn with_additional_options(options: Option<ClientOptions>) -> Self {
        let options = Self::options_for_multiple_mongoses(options, false).await;
        Self::with_options(Some(options)).await
    }

    pub async fn create_user(
        &self,
        user: &str,
        pwd: impl Into<Option<&str>>,
        roles: &[Bson],
        mechanisms: &[AuthMechanism],
        db: impl Into<Option<&str>>,
    ) -> Result<()> {
        let mut cmd = doc! { "createUser": user, "roles": roles };

        if let Some(pwd) = pwd.into() {
            cmd.insert("pwd", pwd);
        }

        if self.server_version_gte(4, 0) && !mechanisms.is_empty() {
            let ms: bson::Array = mechanisms.iter().map(|s| Bson::from(s.as_str())).collect();
            cmd.insert("mechanisms", ms);
        }
        self.database(db.into().unwrap_or("admin"))
            .run_command(cmd, None)
            .await?;
        Ok(())
    }

    pub async fn drop_and_create_user(
        &self,
        user: &str,
        pwd: impl Into<Option<&str>>,
        roles: &[Bson],
        mechanisms: &[AuthMechanism],
        db: Option<&str>,
    ) -> Result<()> {
        let drop_user_result = self
            .database(db.unwrap_or("admin"))
            .run_command(doc! { "dropUser": user }, None)
            .await;

        match drop_user_result.map_err(|e| *e.kind) {
            Err(ErrorKind::Command(CommandError { code: 11, .. })) | Ok(_) => {}
            e @ Err(_) => {
                e.unwrap();
            }
        };

        self.create_user(user, pwd, roles, mechanisms, db).await
    }

    pub fn get_coll(&self, db_name: &str, coll_name: &str) -> Collection<Document> {
        self.database(db_name).collection(coll_name)
    }

    pub async fn init_db_and_coll(&self, db_name: &str, coll_name: &str) -> Collection<Document> {
        let coll = self.get_coll(db_name, coll_name);
        drop_collection(&coll).await;
        coll
    }

    pub async fn init_db_and_typed_coll<T>(&self, db_name: &str, coll_name: &str) -> Collection<T>
    where
        T: Serialize + DeserializeOwned + Unpin + Debug,
    {
        let coll = self.database(db_name).collection(coll_name);
        drop_collection(&coll).await;
        coll
    }

    pub fn get_coll_with_options(
        &self,
        db_name: &str,
        coll_name: &str,
        options: CollectionOptions,
    ) -> Collection<Document> {
        self.database(db_name)
            .collection_with_options(coll_name, options)
    }

    pub async fn init_db_and_coll_with_options(
        &self,
        db_name: &str,
        coll_name: &str,
        options: CollectionOptions,
    ) -> Collection<Document> {
        let coll = self.get_coll_with_options(db_name, coll_name, options);
        drop_collection(&coll).await;
        coll
    }

    pub async fn create_fresh_collection(
        &self,
        db_name: &str,
        coll_name: &str,
        options: impl Into<Option<CreateCollectionOptions>>,
    ) -> Collection<Document> {
        self.drop_collection(db_name, coll_name).await;
        self.database(db_name)
            .create_collection(coll_name, options)
            .await
            .unwrap();

        self.get_coll(db_name, coll_name)
    }

    pub fn supports_fail_command(&self) -> bool {
        let version = if self.is_sharded() {
            VersionReq::parse(">= 4.1.5").unwrap()
        } else {
            VersionReq::parse(">= 4.0").unwrap()
        };
        version.matches(&self.server_version)
    }

    pub fn supports_block_connection(&self) -> bool {
        let version = VersionReq::parse(">= 4.2.9").unwrap();
        version.matches(&self.server_version)
    }

    pub fn supports_fail_command_appname(&self) -> bool {
        let version = VersionReq::parse(">= 4.2.9").unwrap();
        version.matches(&self.server_version)
    }

    pub fn supports_transactions(&self) -> bool {
        self.is_replica_set() && self.server_version_gte(4, 0)
            || self.is_sharded() && self.server_version_gte(4, 2)
    }

    pub async fn enable_failpoint(
        &self,
        fp: FailPoint,
        criteria: impl Into<Option<SelectionCriteria>>,
    ) -> Result<FailPointGuard> {
        fp.enable(self, criteria).await
    }

    pub fn auth_enabled(&self) -> bool {
        self.options.credential.is_some()
    }

    pub fn is_standalone(&self) -> bool {
        self.base_topology() == Topology::Single
    }

    pub fn is_replica_set(&self) -> bool {
        self.base_topology() == Topology::ReplicaSet
    }

    pub fn is_sharded(&self) -> bool {
        self.base_topology() == Topology::Sharded
    }

    pub fn is_load_balanced(&self) -> bool {
        self.base_topology() == Topology::LoadBalanced
    }

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

    /// Returns the `Topology' that can be determined without a server query, i.e. all except
    /// `Toplogy::ShardedReplicaSet`.
    fn base_topology(&self) -> Topology {
        if self.options.load_balanced.unwrap_or(false) {
            return Topology::LoadBalanced;
        }
        if self.server_info.msg.as_deref() == Some("isdbgrid") {
            return Topology::Sharded;
        }
        if self.server_info.set_name.is_some() {
            return Topology::ReplicaSet;
        }
        Topology::Single
    }

    pub async fn topology(&self) -> Topology {
        let bt = self.base_topology();
        if let Topology::Sharded = bt {
            let shard_info = self
                .database("config")
                .collection::<Document>("shards")
                .find_one(None, None)
                .await
                .unwrap()
                .unwrap();
            let hosts = shard_info.get_str("host").unwrap();
            // If the host string has more than one host, a slash will separate the replica set name
            // and list of hosts.
            if hosts.contains('/') {
                return Topology::ShardedReplicaSet;
            }
        }
        bt
    }

    pub fn topology_string(&self) -> String {
        match self.base_topology() {
            Topology::LoadBalanced => "load-balanced",
            Topology::Sharded | Topology::ShardedReplicaSet => "sharded",
            Topology::ReplicaSet => "replicaset",
            Topology::Single => "single",
        }
        .to_string()
    }

    pub async fn options_for_multiple_mongoses(
        options: Option<ClientOptions>,
        use_multiple_mongoses: bool,
    ) -> ClientOptions {
        let is_load_balanced = options
            .as_ref()
            .and_then(|o| o.load_balanced)
            .or(CLIENT_OPTIONS.get().await.load_balanced)
            .unwrap_or(false);
        let default_options = if is_load_balanced {
            // for serverless testing, ignore use_multiple_mongoses.
            let uri = if use_multiple_mongoses && !*SERVERLESS {
                LOAD_BALANCED_MULTIPLE_URI
                    .as_ref()
                    .expect("MULTI_MONGOS_LB_URI is required")
            } else {
                LOAD_BALANCED_SINGLE_URI
                    .as_ref()
                    .expect("SINGLE_MONGOS_LB_URI is required")
            };
            let mut o = ClientOptions::parse_uri(uri, None).await.unwrap();
            update_options_for_testing(&mut o);
            o
        } else {
            CLIENT_OPTIONS.get().await.clone()
        };
        let mut options = match options {
            Some(mut options) => {
                options.merge(default_options);
                options
            }
            None => default_options,
        };
        if Self::new().await.is_sharded() && !use_multiple_mongoses {
            options.hosts = options.hosts.iter().cloned().take(1).collect();
        }
        options
    }
}

pub async fn drop_collection<T>(coll: &Collection<T>)
where
    T: Serialize + DeserializeOwned + Unpin + Debug,
{
    match coll.drop(None).await.map_err(|e| *e.kind) {
        Err(ErrorKind::Command(CommandError { code: 26, .. })) | Ok(_) => {}
        e @ Err(_) => {
            e.unwrap();
        }
    };
}

#[derive(Debug, Deserialize)]
struct BuildInfo {
    version: String,
}

pub fn get_default_name(description: &str) -> String {
    let mut db_name = description
        .replace('$', "%")
        .replace(' ', "_")
        .replace('.', "_");
    // database names must have fewer than 38 characters
    db_name.truncate(37);
    db_name
}

/// Log a message on stderr that won't be captured by `cargo test`.  Panics if the write fails.
pub fn log_uncaptured<S: AsRef<str>>(text: S) {
    use std::io::Write;

    let mut stderr = std::io::stderr();
    stderr.write_all(text.as_ref().as_bytes()).unwrap();
    stderr.write_all(b"\n").unwrap();
}
