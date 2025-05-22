mod event;
pub(crate) mod event_buffer;
pub(crate) mod fail_point;
mod matchable;
#[cfg(feature = "tracing-unstable")]
mod trace;

use std::{env, fmt::Debug, fs::File, future::IntoFuture, io::Write, time::Duration};

use futures::FutureExt;
use serde::{de::DeserializeOwned, Serialize};

#[cfg(feature = "in-use-encryption")]
use crate::client::EncryptedClientBuilder;
use crate::{
    bson::{doc, Bson, Document},
    error::Result,
    hello::{hello_command, HelloCommandResponse},
    options::{AuthMechanism, ClientOptions, CollectionOptions, CreateCollectionOptions},
    test::{get_client_options, server_version_gte, topology_is_sharded},
    BoxFuture,
    Client,
    Collection,
};

#[cfg(feature = "tracing-unstable")]
pub(crate) use self::trace::{
    max_verbosity_levels_for_test_case,
    TracingEvent,
    TracingEventValue,
    TracingHandler,
};
pub(crate) use self::{
    event::{Event, EventClient},
    matchable::{assert_matches, eq_matches, is_expected_type, MatchErrExt, Matchable},
};

#[derive(Clone, Debug)]
pub(crate) struct TestClient {
    client: Client,
}

impl std::ops::Deref for TestClient {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl Client {
    pub(crate) fn for_test() -> TestClientBuilder {
        TestClientBuilder {
            options: None,
            min_heartbeat_freq: None,
            #[cfg(feature = "in-use-encryption")]
            encrypted: None,
            use_single_mongos: false,
        }
    }
}

pub(crate) struct TestClientBuilder {
    options: Option<ClientOptions>,
    min_heartbeat_freq: Option<Duration>,
    #[cfg(feature = "in-use-encryption")]
    encrypted: Option<crate::client::csfle::options::AutoEncryptionOptions>,
    use_single_mongos: bool,
}

impl TestClientBuilder {
    pub(crate) fn options(mut self, options: impl Into<Option<ClientOptions>>) -> Self {
        let options = options.into();
        assert!(self.options.is_none() || options.is_none());
        self.options = options;
        self
    }

    /// When running against a sharded topology, only use the first configured host.
    pub(crate) fn use_single_mongos(mut self) -> Self {
        self.use_single_mongos = true;
        self
    }

    #[cfg(feature = "in-use-encryption")]
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
}

impl IntoFuture for TestClientBuilder {
    type Output = TestClient;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            let mut options = match self.options {
                Some(options) => options,
                None => get_client_options().await.clone(),
            };

            if let Some(freq) = self.min_heartbeat_freq {
                options.test_options_mut().min_heartbeat_freq = Some(freq);
            }

            if self.use_single_mongos && topology_is_sharded().await {
                options.hosts = options.hosts.iter().take(1).cloned().collect();
            }

            #[cfg(feature = "in-use-encryption")]
            let client = match self.encrypted {
                None => Client::with_options(options).unwrap(),
                Some(aeo) => EncryptedClientBuilder::new(options, aeo)
                    .build()
                    .await
                    .unwrap(),
            };
            #[cfg(not(feature = "in-use-encryption"))]
            let client = Client::with_options(options).unwrap();

            TestClient { client }
        }
        .boxed()
    }
}

impl TestClient {
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

        if server_version_gte(4, 0).await && !mechanisms.is_empty() {
            let ms: crate::bson::Array =
                mechanisms.iter().map(|s| Bson::from(s.as_str())).collect();
            cmd.insert("mechanisms", ms);
        }
        self.database(db.into().unwrap_or("admin"))
            .run_command(cmd)
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
        coll.drop().await.unwrap();
        coll
    }

    pub(crate) async fn init_db_and_typed_coll<T>(
        &self,
        db_name: &str,
        coll_name: &str,
    ) -> Collection<T>
    where
        T: Serialize + DeserializeOwned + Unpin + Debug + Send + Sync,
    {
        let coll = self.database(db_name).collection(coll_name);
        coll.drop().await.unwrap();
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

    pub(crate) async fn create_fresh_collection(
        &self,
        db_name: &str,
        coll_name: &str,
        options: impl Into<Option<CreateCollectionOptions>>,
    ) -> Collection<Document> {
        self.drop_collection(db_name, coll_name).await;
        self.database(db_name)
            .create_collection(coll_name)
            .with_options(options)
            .await
            .unwrap();

        self.get_coll(db_name, coll_name)
    }

    pub(crate) async fn drop_collection(&self, db_name: &str, coll_name: &str) {
        let coll = self.get_coll(db_name, coll_name);
        coll.drop().await.unwrap();
    }

    #[allow(dead_code)]
    pub(crate) fn into_client(self) -> Client {
        self.client
    }

    pub(crate) async fn hello(&self) -> Result<HelloCommandResponse> {
        let hello = hello_command(
            self.options().server_api.as_ref(),
            self.options().load_balanced,
            None,
            None,
        );
        let hello_response_doc = self
            .database("admin")
            .run_command(hello.body.try_into()?)
            .await?;
        Ok(crate::bson::from_document(hello_response_doc)?)
    }
}

pub(crate) fn get_default_name(description: &str) -> String {
    let mut db_name = description.replace('$', "%").replace([' ', '.'], "_");
    // database names must have fewer than 38 characters
    db_name.truncate(37);
    db_name
}

#[test]
fn get_exe_name_skip_ci() {
    let mut file = File::create("exe_name.txt").expect("Failed to create file");
    let exe_name = env::current_exe()
        .expect("Failed to determine name of test executable")
        .into_os_string()
        .into_string()
        .expect("Failed to convert OS string to string");
    write!(file, "{}", exe_name).expect("Failed to write executable name to file");
}

/// Log a message on stderr that won't be captured by `cargo test`.  Panics if the write fails.
pub(crate) fn log_uncaptured<S: AsRef<str>>(text: S) {
    use std::io::Write;

    let mut stderr = std::io::stderr();
    let mut sinks = vec![&mut stderr as &mut dyn Write];
    let mut other;
    let other_path = std::env::var("LOG_UNCAPTURED").unwrap_or("/dev/tty".to_string());
    if let Ok(f) = std::fs::OpenOptions::new().append(true).open(other_path) {
        other = f;
        sinks.push(&mut other);
    }

    for sink in sinks {
        sink.write_all(text.as_ref().as_bytes()).unwrap();
        sink.write_all(b"\n").unwrap();
    }
}

#[test]
fn log_uncaptured_example() {
    println!("this is captured");
    log_uncaptured("this is not captured");
}

pub(crate) fn file_level_log(message: impl AsRef<str>) {
    log_uncaptured(format!("\n------------\n{}\n", message.as_ref()));
}
