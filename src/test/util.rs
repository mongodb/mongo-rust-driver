mod event;
mod failpoint;
mod lock;
mod matchable;
mod subscriber;
#[cfg(feature = "tracing-unstable")]
mod trace;

pub(crate) use self::{
    event::{Event, EventClient, EventHandler, SdamEvent},
    failpoint::{FailCommandOptions, FailPoint, FailPointGuard, FailPointMode},
    lock::TestLock,
    matchable::{assert_matches, eq_matches, is_expected_type, MatchErrExt, Matchable},
    subscriber::EventSubscriber,
};

#[cfg(feature = "tracing-unstable")]
pub(crate) use self::trace::{
    max_verbosity_levels_for_test_case,
    TracingEvent,
    TracingEventValue,
    TracingHandler,
};

use std::{fmt::Debug, sync::Arc, time::Duration};

#[cfg(feature = "in-use-encryption-unstable")]
use crate::client::EncryptedClientBuilder;
use crate::{
    bson::{doc, Bson},
    client::options::ServerAddress,
    hello::{hello_command, HelloCommandResponse},
    selection_criteria::SelectionCriteria,
};
use bson::Document;
use semver::{Version, VersionReq};
use serde::{de::DeserializeOwned, Serialize};

use super::CLIENT_OPTIONS;
use crate::{
    error::{CommandError, ErrorKind, Result},
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
pub(crate) struct TestClient {
    client: Client,
    pub(crate) server_info: HelloCommandResponse,
    pub(crate) server_version: Version,
    pub(crate) server_parameters: Document,
}

impl std::ops::Deref for TestClient {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl Client {
    pub(crate) fn test_builder() -> TestClientBuilder {
        TestClientBuilder {
            options: None,
            handler: None,
            min_heartbeat_freq: None,
            #[cfg(feature = "in-use-encryption-unstable")]
            encrypted: None,
        }
    }
}

pub(crate) struct TestClientBuilder {
    options: Option<ClientOptions>,
    handler: Option<Arc<EventHandler>>,
    min_heartbeat_freq: Option<Duration>,
    #[cfg(feature = "in-use-encryption-unstable")]
    encrypted: Option<crate::client::csfle::options::AutoEncryptionOptions>,
}

impl TestClientBuilder {
    pub(crate) fn options(mut self, options: impl Into<Option<ClientOptions>>) -> Self {
        let options = options.into();
        assert!(self.options.is_none() || options.is_none());
        self.options = options;
        self
    }

    /// Modify options via `TestClient::options_for_multiple_mongoses` before setting them.
    // TODO RUST-1449 Simplify or remove this entirely.
    pub(crate) async fn additional_options(
        mut self,
        options: impl Into<Option<ClientOptions>>,
        use_multiple_mongoses: bool,
    ) -> Self {
        let options = options.into();
        assert!(self.options.is_none() || options.is_none());
        self.options =
            Some(TestClient::options_for_multiple_mongoses(options, use_multiple_mongoses).await);
        self
    }

    pub(crate) fn event_handler(mut self, handler: impl Into<Option<Arc<EventHandler>>>) -> Self {
        let handler = handler.into();
        assert!(self.handler.is_none() || handler.is_none());
        self.handler = handler;
        self
    }

    #[cfg(feature = "in-use-encryption-unstable")]
    pub(crate) fn encrypted_options(
        mut self,
        encrypted: crate::client::csfle::options::AutoEncryptionOptions,
    ) -> Self {
        assert!(self.encrypted.is_none());
        self.encrypted = Some(encrypted);
        self
    }

    pub(crate) fn min_heartbeat_freq(
        mut self,
        min_heartbeat_freq: impl Into<Option<Duration>>,
    ) -> Self {
        let min_heartbeat_freq = min_heartbeat_freq.into();
        assert!(self.min_heartbeat_freq.is_none() || min_heartbeat_freq.is_none());
        self.min_heartbeat_freq = min_heartbeat_freq;
        self
    }

    pub(crate) async fn build(self) -> TestClient {
        let mut options = match self.options {
            Some(options) => options,
            None => CLIENT_OPTIONS.get().await.clone(),
        };

        if let Some(handler) = self.handler {
            options.command_event_handler = Some(handler.clone());
            options.cmap_event_handler = Some(handler.clone());
            options.sdam_event_handler = Some(handler);
        }

        if let Some(freq) = self.min_heartbeat_freq {
            options.test_options_mut().min_heartbeat_freq = Some(freq);
        }

        #[cfg(feature = "in-use-encryption-unstable")]
        let client = match self.encrypted {
            None => Client::with_options(options).unwrap(),
            Some(aeo) => EncryptedClientBuilder::new(options, aeo)
                .build()
                .await
                .unwrap(),
        };
        #[cfg(not(feature = "in-use-encryption-unstable"))]
        let client = Client::with_options(options).unwrap();

        TestClient::from_client(client).await
    }

    pub(crate) fn handler(&self) -> Option<&Arc<EventHandler>> {
        self.handler.as_ref()
    }
}

impl TestClient {
    // TODO RUST-1449 Remove uses of direct constructors in favor of `TestClientBuilder`.
    pub(crate) async fn new() -> Self {
        Self::with_options(None).await
    }

    pub(crate) async fn with_options(options: impl Into<Option<ClientOptions>>) -> Self {
        Self::with_handler(None, options).await
    }

    pub(crate) async fn with_handler(
        event_handler: Option<Arc<EventHandler>>,
        options: impl Into<Option<ClientOptions>>,
    ) -> Self {
        Client::test_builder()
            .options(options)
            .event_handler(event_handler)
            .build()
            .await
    }

    async fn from_client(client: Client) -> Self {
        let hello = hello_command(
            client.options().server_api.as_ref(),
            client.options().load_balanced,
            None,
            None,
        );
        let server_info_doc = client
            .database("admin")
            .run_command(hello.body, None)
            .await
            .unwrap();
        let server_info = bson::from_document(server_info_doc).unwrap();

        let build_info = client
            .database("test")
            .run_command(doc! { "buildInfo": 1 }, None)
            .await
            .unwrap();
        let mut server_version = Version::parse(build_info.get_str("version").unwrap()).unwrap();
        // Clear prerelease tag to allow version comparisons.
        server_version.pre = semver::Prerelease::EMPTY;

        let server_parameters = client
            .database("admin")
            .run_command(doc! { "getParameter": "*" }, None)
            .await
            .unwrap_or_default();

        Self {
            client,
            server_info,
            server_version,
            server_parameters,
        }
    }

    pub(crate) async fn with_additional_options(options: Option<ClientOptions>) -> Self {
        Client::test_builder()
            .additional_options(options, false)
            .await
            .build()
            .await
    }

    pub(crate) async fn create_user(
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

    pub(crate) fn get_coll(&self, db_name: &str, coll_name: &str) -> Collection<Document> {
        self.database(db_name).collection(coll_name)
    }

    pub(crate) async fn init_db_and_coll(
        &self,
        db_name: &str,
        coll_name: &str,
    ) -> Collection<Document> {
        let coll = self.get_coll(db_name, coll_name);
        drop_collection(&coll).await;
        coll
    }

    pub(crate) async fn init_db_and_typed_coll<T>(
        &self,
        db_name: &str,
        coll_name: &str,
    ) -> Collection<T>
    where
        T: Serialize + DeserializeOwned + Unpin + Debug,
    {
        let coll = self.database(db_name).collection(coll_name);
        drop_collection(&coll).await;
        coll
    }

    pub(crate) fn get_coll_with_options(
        &self,
        db_name: &str,
        coll_name: &str,
        options: CollectionOptions,
    ) -> Collection<Document> {
        self.database(db_name)
            .collection_with_options(coll_name, options)
    }

    pub(crate) async fn init_db_and_coll_with_options(
        &self,
        db_name: &str,
        coll_name: &str,
        options: CollectionOptions,
    ) -> Collection<Document> {
        let coll = self.get_coll_with_options(db_name, coll_name, options);
        drop_collection(&coll).await;
        coll
    }

    pub(crate) async fn create_fresh_collection(
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

    pub(crate) fn supports_fail_command(&self) -> bool {
        let version = if self.is_sharded() {
            ">= 4.1.5"
        } else {
            ">= 4.0"
        };
        self.server_version_matches(version)
    }

    pub(crate) fn server_version_matches(&self, req: &str) -> bool {
        VersionReq::parse(req)
            .unwrap()
            .matches(&self.server_version)
    }

    pub(crate) fn supports_block_connection(&self) -> bool {
        self.server_version_matches(">= 4.2.9")
    }

    /// Whether the deployment supports failing the initial handshake
    /// only when it uses a specified appName.
    ///
    /// See SERVER-49336 for more info.
    pub(crate) fn supports_fail_command_appname_initial_handshake(&self) -> bool {
        let requirements = [
            VersionReq::parse(">= 4.2.15, < 4.3.0").unwrap(),
            VersionReq::parse(">= 4.4.7, < 4.5.0").unwrap(),
            VersionReq::parse(">= 4.9.0").unwrap(),
        ];
        requirements
            .iter()
            .any(|req| req.matches(&self.server_version))
    }

    pub(crate) fn supports_transactions(&self) -> bool {
        self.is_replica_set() && self.server_version_gte(4, 0)
            || self.is_sharded() && self.server_version_gte(4, 2)
    }

    pub(crate) fn supports_streaming_monitoring_protocol(&self) -> bool {
        self.server_info.topology_version.is_some()
    }

    pub(crate) async fn enable_failpoint(
        &self,
        fp: FailPoint,
        criteria: impl Into<Option<SelectionCriteria>>,
    ) -> Result<FailPointGuard> {
        fp.enable(self, criteria).await
    }

    pub(crate) fn auth_enabled(&self) -> bool {
        self.client.options().credential.is_some()
    }

    pub(crate) fn is_standalone(&self) -> bool {
        self.topology() == Topology::Single
    }

    pub(crate) fn is_replica_set(&self) -> bool {
        self.topology() == Topology::ReplicaSet
    }

    pub(crate) fn is_sharded(&self) -> bool {
        self.topology() == Topology::Sharded
    }

    pub(crate) fn is_load_balanced(&self) -> bool {
        self.topology() == Topology::LoadBalanced
    }

    pub(crate) fn server_version_eq(&self, major: u64, minor: u64) -> bool {
        self.server_version.major == major && self.server_version.minor == minor
    }

    #[allow(dead_code)]
    pub(crate) fn server_version_gt(&self, major: u64, minor: u64) -> bool {
        self.server_version.major > major
            || (self.server_version.major == major && self.server_version.minor > minor)
    }

    pub(crate) fn server_version_gte(&self, major: u64, minor: u64) -> bool {
        self.server_version.major > major
            || (self.server_version.major == major && self.server_version.minor >= minor)
    }

    pub(crate) fn server_version_lt(&self, major: u64, minor: u64) -> bool {
        self.server_version.major < major
            || (self.server_version.major == major && self.server_version.minor < minor)
    }

    #[allow(dead_code)]
    pub(crate) fn server_version_lte(&self, major: u64, minor: u64) -> bool {
        self.server_version.major < major
            || (self.server_version.major == major && self.server_version.minor <= minor)
    }

    pub(crate) async fn drop_collection(&self, db_name: &str, coll_name: &str) {
        let coll = self.get_coll(db_name, coll_name);
        drop_collection(&coll).await;
    }

    /// Returns the `Topology' that can be determined without a server query, i.e. all except
    /// `Toplogy::ShardedReplicaSet`.
    pub(crate) fn topology(&self) -> Topology {
        if self.client.options().load_balanced.unwrap_or(false) {
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

    pub(crate) fn topology_string(&self) -> String {
        match self.topology() {
            Topology::LoadBalanced => "load-balanced",
            Topology::Sharded => "sharded",
            Topology::ReplicaSet => "replicaset",
            Topology::Single => "single",
        }
        .to_string()
    }

    pub(crate) fn primary(&self) -> Option<ServerAddress> {
        self.server_info
            .primary
            .as_ref()
            .map(|s| ServerAddress::parse(s).unwrap())
    }

    pub(crate) async fn options_for_multiple_mongoses(
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
            options.hosts = options.hosts.iter().take(1).cloned().collect();
        }
        options
    }

    #[allow(dead_code)]
    pub(crate) fn into_client(self) -> Client {
        self.client
    }
}

pub(crate) async fn drop_collection<T>(coll: &Collection<T>)
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

pub(crate) fn get_default_name(description: &str) -> String {
    let mut db_name = description.replace('$', "%").replace([' ', '.'], "_");
    // database names must have fewer than 38 characters
    db_name.truncate(37);
    db_name
}

/// Log a message on stderr that won't be captured by `cargo test`.  Panics if the write fails.
pub(crate) fn log_uncaptured<S: AsRef<str>>(text: S) {
    use std::io::Write;

    let mut stderr = std::io::stderr();
    stderr.write_all(text.as_ref().as_bytes()).unwrap();
    stderr.write_all(b"\n").unwrap();
}

pub(crate) fn file_level_log(message: impl AsRef<str>) {
    log_uncaptured(format!("\n------------\n{}\n", message.as_ref()));
}
