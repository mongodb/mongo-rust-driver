#[cfg(feature = "in-use-encryption-unstable")]
mod csfle;
pub(crate) mod operation;
pub(crate) mod test_event;
pub(crate) mod test_file;

use std::{future::IntoFuture, ops::Deref, sync::Arc, time::Duration};

use futures::{future::BoxFuture, FutureExt};
use semver::VersionReq;

use crate::{
    bson::{doc, from_bson},
    coll::options::{DistinctOptions, DropCollectionOptions},
    concern::WriteConcern,
    options::{ClientOptions, CreateCollectionOptions, InsertManyOptions},
    runtime,
    sdam::{ServerInfo, MIN_HEARTBEAT_FREQUENCY},
    selection_criteria::SelectionCriteria,
    test::{
        file_level_log,
        get_client_options,
        log_uncaptured,
        spec::deserialize_spec_tests,
        util::{get_default_name, FailPointGuard},
        EventClient,
        TestClient,
        SERVERLESS,
    },
    Client,
    ClientSession,
    Namespace,
};

use operation::OperationObject;
use test_event::CommandStartedEvent;
use test_file::{TestData, TestFile};

use super::Operation;

const SKIPPED_OPERATIONS: &[&str] = &[
    "bulkWrite",
    "count",
    // TODO RUST-1657: unskip the download operations
    "download",
    "download_by_name",
    "listCollectionObjects",
    "listDatabaseObjects",
    "mapReduce",
];

pub(crate) fn run_v2_tests(spec: &'static [&'static str]) -> RunV2TestsAction {
    RunV2TestsAction {
        spec,
        skipped_files: None,
    }
}

pub(crate) struct RunV2TestsAction {
    spec: &'static [&'static str],
    skipped_files: Option<Vec<&'static str>>,
}

impl RunV2TestsAction {
    pub(crate) fn skip_files(self, skipped_files: &[&'static str]) -> Self {
        Self {
            skipped_files: Some(skipped_files.to_vec()),
            ..self
        }
    }
}

impl IntoFuture for RunV2TestsAction {
    type Output = ();
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            for (test_file, path) in
                deserialize_spec_tests::<TestFile>(self.spec, self.skipped_files.as_deref())
            {
                run_v2_test(path, test_file).await;
            }
        }
        .boxed()
    }
}

struct FileContext {
    internal_client: TestClient,
    is_csfle_test: bool,
}

impl FileContext {
    async fn new(path: &std::path::Path) -> Self {
        let internal_client = TestClient::new().await;
        let is_csfle_test = path.to_string_lossy().contains("client-side-encryption");

        Self {
            internal_client,
            is_csfle_test,
        }
    }

    fn check_topology(&self, test_file: &TestFile) -> bool {
        if let Some(requirements) = &test_file.run_on {
            return requirements
                .iter()
                .any(|run_on| run_on.can_run_on(&self.internal_client));
        }
        true
    }
}

struct TestContext {
    description: String,
    ns: Namespace,
    internal_client: TestClient,
    client: EventClient,
    fail_point_guards: Vec<FailPointGuard>,
    session0: Option<ClientSession>,
    session1: Option<ClientSession>,
}

impl TestContext {
    async fn new(
        test_file: &TestFile,
        test: &test_file::Test,
        internal_client: &TestClient,
    ) -> Self {
        // Get the test target collection
        let db_name = test_file
            .database_name
            .clone()
            .unwrap_or_else(|| get_default_name(&test.description));
        let coll_name = test_file
            .collection_name
            .clone()
            .unwrap_or_else(|| get_default_name(&test.description));
        let coll = internal_client.database(&db_name).collection(&coll_name);

        // Reset the test collection as needed
        #[allow(unused_mut)]
        let mut options = DropCollectionOptions::builder()
            .write_concern(WriteConcern::majority())
            .build();
        #[cfg(feature = "in-use-encryption-unstable")]
        if let Some(enc_fields) = &test_file.encrypted_fields {
            options.encrypted_fields = Some(enc_fields.clone());
        }
        let req = VersionReq::parse(">=4.7").unwrap();
        if !(db_name.as_str() == "admin"
            && internal_client.is_sharded()
            && req.matches(&internal_client.server_version))
        {
            coll.drop(options).await.unwrap();
        }

        #[allow(unused_mut)]
        let mut options = CreateCollectionOptions::builder()
            .write_concern(WriteConcern::majority())
            .build();
        #[cfg(feature = "in-use-encryption-unstable")]
        {
            if let Some(schema) = &test_file.json_schema {
                options.validator = Some(doc! { "$jsonSchema": schema });
            }
            if let Some(enc_fields) = &test_file.encrypted_fields {
                options.encrypted_fields = Some(enc_fields.clone());
            }
        }
        internal_client
            .database(&db_name)
            .create_collection(&coll_name, options)
            .await
            .unwrap();

        // Insert test data
        if let Some(data) = &test_file.data {
            match data {
                TestData::Single(data) => {
                    if !data.is_empty() {
                        let options = InsertManyOptions::builder()
                            .write_concern(WriteConcern::majority())
                            .build();
                        coll.insert_many(data.clone(), options).await.unwrap();
                    }
                }
                TestData::Many(_) => panic!("{}: invalid data format", &test.description),
            }
        }

        // Construct the test client
        let mut additional_options = match &test.client_options {
            Some(opts) => ClientOptions::parse_uri(&opts.uri, None).await.unwrap(),
            None => ClientOptions::builder()
                .hosts(get_client_options().await.hosts.clone())
                .build(),
        };
        if additional_options.heartbeat_freq.is_none() {
            additional_options.heartbeat_freq = Some(MIN_HEARTBEAT_FREQUENCY);
        }
        let builder = Client::test_builder()
            .additional_options(
                additional_options,
                test.use_multiple_mongoses.unwrap_or(false),
            )
            .await
            .min_heartbeat_freq(Some(Duration::from_millis(50)));
        #[cfg(feature = "in-use-encryption-unstable")]
        let builder = csfle::set_auto_enc(builder, test);
        let client = builder.event_client().build().await;

        // TODO RUST-900: Remove this extraneous call.
        if internal_client.is_sharded()
            && internal_client.server_version_lte(4, 2)
            && test.operations.iter().any(|op| op.name == "distinct")
        {
            for server_address in internal_client.options().hosts.clone() {
                let options = DistinctOptions::builder()
                    .selection_criteria(Some(SelectionCriteria::Predicate(Arc::new(
                        move |server_info: &ServerInfo| *server_info.address() == server_address,
                    ))))
                    .build();
                coll.distinct("_id", None, options).await.unwrap();
            }
        }

        // Persist fail point guards so they disable post-test.
        let mut fail_point_guards: Vec<FailPointGuard> = Vec::new();
        if let Some(fail_point) = &test.fail_point {
            fail_point_guards.push(fail_point.enable(client.deref(), None).await.unwrap());
        }

        // Start the test sessions
        let options = match test.session_options {
            Some(ref options) => options.get("session0").cloned(),
            None => None,
        };
        let session0 = Some(client.start_session().with_options(options).await.unwrap());

        let options = match test.session_options {
            Some(ref options) => options.get("session1").cloned(),
            None => None,
        };
        let session1 = Some(client.start_session().with_options(options).await.unwrap());

        Self {
            description: test.description.clone(),
            ns: Namespace {
                db: db_name,
                coll: coll_name,
            },
            internal_client: internal_client.clone(),
            client,
            fail_point_guards,
            session0,
            session1,
        }
    }

    async fn run_operation(
        &mut self,
        operation: &Operation,
    ) -> Option<Result<Option<bson::Bson>, crate::error::Error>> {
        if operation.name == "endSession" {
            let session = match &operation.object {
                Some(OperationObject::Session0) => &mut self.session0,
                Some(OperationObject::Session1) => &mut self.session1,
                other => panic!("invalid object for `endSession`: {:?}", other),
            };
            drop(session.take());
            runtime::delay_for(Duration::from_secs(1)).await;
            return None;
        }

        let sessions = OpSessions {
            session0: self.session0.as_mut(),
            session1: self.session1.as_mut(),
        };

        let mut runner = OpRunner {
            description: self.description.clone(),
            internal_client: self.internal_client.clone(),
            client: self.client.clone(),
            ns: self.ns.clone(),
            fail_point_guards: &mut self.fail_point_guards,
        };

        runner.run_operation(operation, sessions).await
    }
}

pub(crate) struct OpSessions<'a> {
    session0: Option<&'a mut ClientSession>,
    session1: Option<&'a mut ClientSession>,
}

pub(crate) struct OpRunner<'a> {
    description: String,
    internal_client: TestClient,
    client: EventClient,
    ns: Namespace,
    fail_point_guards: &'a mut Vec<FailPointGuard>,
}

impl<'a> OpRunner<'a> {
    pub(crate) async fn run_operation<'b>(
        &mut self,
        operation: &Operation,
        mut sessions: OpSessions<'b>,
    ) -> Option<Result<Option<bson::Bson>, crate::error::Error>> {
        if operation.name == "withTransaction" {
            if !matches!(&operation.object, Some(OperationObject::Session0)) {
                panic!("invalid object for withTransaction: {:?}", operation.object);
            }
            return Some(operation.execute_recursive(self, sessions).await);
        }

        let db = match &operation.database_options {
            Some(options) => self
                .client
                .database_with_options(&self.ns.db, options.clone()),
            None => self.client.database(&self.ns.db),
        };
        let coll = match &operation.collection_options {
            Some(options) => db.collection_with_options(&self.ns.coll, options.clone()),
            None => db.collection(&self.ns.coll),
        };

        let session = match operation.session.as_deref() {
            Some("session0") => sessions.session0.as_deref_mut(),
            Some("session1") => sessions.session1.as_deref_mut(),
            Some(other) => panic!("unknown session name: {}", other),
            None => None,
        };

        Some(match operation.object {
            Some(OperationObject::Collection) | None => {
                let result = operation.execute_on_collection(&coll, session).await;
                // This test (in src/test/spec/json/sessions/server-support.json) runs two
                // operations with implicit sessions in sequence and then checks to see if they
                // used the same lsid. We delay for one second to ensure that the
                // implicit session used in the first operation is returned to the pool before
                // the second operation is executed.
                if self.description == "Server supports implicit sessions" {
                    runtime::delay_for(Duration::from_secs(1)).await;
                }
                result
            }
            Some(OperationObject::Database) => operation.execute_on_database(&db, session).await,
            Some(OperationObject::Client) => operation.execute_on_client(&self.client).await,
            Some(OperationObject::Session0) => {
                operation
                    .execute_on_session(sessions.session0.as_mut().unwrap())
                    .await
            }
            Some(OperationObject::Session1) => {
                operation
                    .execute_on_session(sessions.session1.as_mut().unwrap())
                    .await
            }
            Some(OperationObject::TestRunner) => {
                match operation.name.as_str() {
                    "assertDifferentLsidOnLastTwoCommands" => {
                        assert_different_lsid_on_last_two_commands(&self.client)
                    }
                    "assertSameLsidOnLastTwoCommands" => {
                        assert_same_lsid_on_last_two_commands(&self.client)
                    }
                    "assertSessionDirty" => {
                        assert!(session.unwrap().is_dirty())
                    }
                    "assertSessionNotDirty" => {
                        assert!(!session.unwrap().is_dirty())
                    }
                    "assertSessionTransactionState"
                    | "assertSessionPinned"
                    | "assertSessionUnpinned" => {
                        operation
                            .execute_on_session(session.unwrap())
                            .await
                            .unwrap();
                    }
                    "assertCollectionExists"
                    | "assertCollectionNotExists"
                    | "assertIndexExists"
                    | "assertIndexNotExists" => {
                        operation
                            .execute_on_client(&self.internal_client)
                            .await
                            .unwrap();
                    }
                    "targetedFailPoint" => {
                        let fail_point = from_bson(
                            operation
                                .execute_on_client(&self.internal_client)
                                .await
                                .unwrap()
                                .unwrap(),
                        )
                        .unwrap();

                        let selection_criteria = session
                            .unwrap()
                            .transaction
                            .pinned_mongos()
                            .cloned()
                            .unwrap_or_else(|| panic!("ClientSession is not pinned"));

                        self.fail_point_guards.push(
                            self.client
                                .deref()
                                .enable_failpoint(fail_point, Some(selection_criteria))
                                .await
                                .unwrap(),
                        );
                    }
                    other => panic!("unknown operation: {}", other),
                }
                return None;
            }
            Some(OperationObject::GridfsBucket) => {
                panic!("unsupported operation: {}", operation.name)
            }
        })
    }
}

async fn run_v2_test(path: std::path::PathBuf, test_file: TestFile) {
    let file_ctx = FileContext::new(&path).await;

    file_level_log(format!("Running tests from {}", path.display(),));

    if !file_ctx.check_topology(&test_file) {
        log_uncaptured("Client topology not compatible with test");
        return;
    }

    for test in &test_file.tests {
        log_uncaptured(format!("Running {}", &test.description));

        if test
            .operations
            .iter()
            .any(|operation| SKIPPED_OPERATIONS.contains(&operation.name.as_str()))
        {
            log_uncaptured(format!(
                "skipping {}: unsupported operation",
                test.description
            ));
            continue;
        }

        if let Some(skip_reason) = &test.skip_reason {
            log_uncaptured(format!("skipping {}: {}", test.description, skip_reason));
            continue;
        }

        // `killAllSessions` isn't supported on serverless.
        // TODO CLOUDP-84298 remove this conditional.
        if !*SERVERLESS {
            match file_ctx
                .internal_client
                .database("admin")
                .run_command(doc! { "killAllSessions": [] }, None)
                .await
            {
                Ok(_) => {}
                Err(err) => match err.sdam_code() {
                    Some(11601) => {}
                    _ => panic!("{}: killAllSessions failed", test.description),
                },
            }
        }

        #[cfg(feature = "in-use-encryption-unstable")]
        csfle::populate_key_vault(&file_ctx.internal_client, test_file.key_vault_data.as_ref())
            .await;

        let mut test_ctx = TestContext::new(&test_file, test, &file_ctx.internal_client).await;
        let session0_lsid = test_ctx.session0.as_ref().unwrap().id().clone();
        let session1_lsid = test_ctx.session1.as_ref().unwrap().id().clone();

        for operation in &test.operations {
            let result = match test_ctx.run_operation(operation).await {
                Some(r) => r,
                None => continue,
            };

            operation.assert_result_matches(&result, &test.description);
        }

        test_ctx.session0.take();
        test_ctx.session1.take();

        // wait for the transaction in progress to be aborted implicitly when the session is dropped
        if test.description.as_str() == "implicit abort" {
            runtime::delay_for(Duration::from_secs(1)).await;
        }

        if let Some(expectations) = &test.expectations {
            let events: Vec<CommandStartedEvent> = test_ctx
                .client
                .get_all_command_started_events()
                .into_iter()
                .map(Into::into)
                .collect();

            assert!(
                events.len() >= expectations.len(),
                "[{}] expected events \n{:#?}\n got events\n{:#?}",
                test.description,
                expectations,
                events
            );
            for (actual_event, expected_event) in events.iter().zip(expectations.iter()) {
                let result =
                    actual_event.matches_expected(expected_event, &session0_lsid, &session1_lsid);
                assert!(
                    result.is_ok(),
                    "[{}] {}",
                    test.description,
                    result.unwrap_err()
                );
            }
        }

        if let Some(outcome) = &test.outcome {
            outcome
                .assert_matches_actual(
                    &test_ctx.ns.db,
                    &test_ctx.ns.coll,
                    if file_ctx.is_csfle_test {
                        &test_ctx.internal_client
                    } else {
                        &test_ctx.client
                    },
                )
                .await;
        }
    }
}

fn assert_different_lsid_on_last_two_commands(client: &EventClient) {
    let events = client.get_all_command_started_events();
    let lsid1 = events[events.len() - 1].command.get("lsid").unwrap();
    let lsid2 = events[events.len() - 2].command.get("lsid").unwrap();
    assert_ne!(lsid1, lsid2);
}

fn assert_same_lsid_on_last_two_commands(client: &EventClient) {
    let events = client.get_all_command_started_events();
    let lsid1 = events[events.len() - 1].command.get("lsid").unwrap();
    let lsid2 = events[events.len() - 2].command.get("lsid").unwrap();
    assert_eq!(lsid1, lsid2);
}
