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
    concern::{Acknowledgment, WriteConcern},
    options::{ClientOptions, CreateCollectionOptions, InsertManyOptions},
    runtime,
    sdam::{ServerInfo, MIN_HEARTBEAT_FREQUENCY},
    selection_criteria::SelectionCriteria,
    test::{
        assert_matches,
        file_level_log,
        log_uncaptured,
        spec::deserialize_spec_tests,
        util::{get_default_name, FailPointGuard},
        EventClient,
        TestClient,
        CLIENT_OPTIONS,
        SERVERLESS,
    },
    Client,
};

use operation::{OperationObject, OperationResult};
use test_event::CommandStartedEvent;
use test_file::{TestData, TestFile};

const SKIPPED_OPERATIONS: &[&str] = &[
    "bulkWrite",
    "count",
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
    skipped_files: Option<&'static [&'static str]>,
}

impl RunV2TestsAction {
    #[cfg(feature = "in-use-encryption-unstable")]
    pub(crate) fn skip_files(self, skipped_files: &'static [&'static str]) -> Self {
        Self {
            skipped_files: Some(skipped_files),
            ..self
        }
    }
}

impl IntoFuture for RunV2TestsAction {
    type Output = ();
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            for (path, test_file) in
                deserialize_spec_tests::<TestFile>(self.spec, self.skipped_files)
            {
                run_v2_test(test_file, path).await;
            }
        }
        .boxed()
    }
}

async fn run_v2_test(path: std::path::PathBuf, test_file: TestFile) {
    let internal_client = TestClient::new().await;

    file_level_log(format!("Running tests from {}", path.display(),));
    let is_csfle_test = path.to_string_lossy().contains("client-side-encryption");

    if let Some(requirements) = test_file.run_on {
        let can_run_on = requirements
            .iter()
            .any(|run_on| run_on.can_run_on(&internal_client));
        if !can_run_on {
            log_uncaptured("Client topology not compatible with test");
            return;
        }
    }

    for test in test_file.tests {
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

        if let Some(skip_reason) = test.skip_reason {
            log_uncaptured(format!("skipping {}: {}", test.description, skip_reason));
            continue;
        }

        // `killAllSessions` isn't supported on serverless.
        // TODO CLOUDP-84298 remove this conditional.
        if !*SERVERLESS {
            match internal_client
                .database("admin")
                .run_command(doc! { "killAllSessions": [] }, None)
                .await
            {
                Ok(_) => {}
                Err(err) => match err.code() {
                    Some(11601) => {}
                    _ => panic!("{}: killAllSessions failed", test.description),
                },
            }
        }

        #[cfg(feature = "in-use-encryption-unstable")]
        csfle::populate_key_vault(&internal_client, test_file.key_vault_data.as_ref()).await;

        let db_name = test_file
            .database_name
            .clone()
            .unwrap_or_else(|| get_default_name(&test.description));
        let coll_name = test_file
            .collection_name
            .clone()
            .unwrap_or_else(|| get_default_name(&test.description));

        let coll = internal_client.database(&db_name).collection(&coll_name);
        #[allow(unused_mut)]
        let mut options = DropCollectionOptions::builder()
            .write_concern(majority_write_concern())
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
            .write_concern(majority_write_concern())
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

        if let Some(data) = &test_file.data {
            match data {
                TestData::Single(data) => {
                    if !data.is_empty() {
                        let options = InsertManyOptions::builder()
                            .write_concern(majority_write_concern())
                            .build();
                        coll.insert_many(data.clone(), options).await.unwrap();
                    }
                }
                TestData::Many(_) => panic!("{}: invalid data format", &test.description),
            }
        }

        let mut additional_options = match &test.client_options {
            Some(opts) => ClientOptions::parse_uri(&opts.uri, None).await.unwrap(),
            None => ClientOptions::builder()
                .hosts(CLIENT_OPTIONS.get().await.hosts.clone())
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
        let builder = csfle::set_auto_enc(builder, &test);
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

        let mut fail_point_guards: Vec<FailPointGuard> = Vec::new();
        if let Some(fail_point) = test.fail_point {
            fail_point_guards.push(fail_point.enable(client.deref(), None).await.unwrap());
        }

        let options = match test.session_options {
            Some(ref options) => options.get("session0").cloned(),
            None => None,
        };
        let mut session0 = Some(client.start_session(options).await.unwrap());
        let session0_lsid = session0.as_ref().unwrap().id().clone();

        let options = match test.session_options {
            Some(ref options) => options.get("session1").cloned(),
            None => None,
        };
        let mut session1 = Some(client.start_session(options).await.unwrap());
        let session1_lsid = session1.as_ref().unwrap().id().clone();

        for operation in test.operations {
            let db = match &operation.database_options {
                Some(options) => client.database_with_options(&db_name, options.clone()),
                None => client.database(&db_name),
            };
            let coll = match &operation.collection_options {
                Some(options) => db.collection_with_options(&coll_name, options.clone()),
                None => db.collection(&coll_name),
            };

            let session = match operation.session.as_deref() {
                Some("session0") => session0.as_mut(),
                Some("session1") => session1.as_mut(),
                Some(other) => panic!("unknown session name: {}", other),
                None => None,
            };

            let result = match operation.object {
                Some(OperationObject::Collection) | None => {
                    let result = operation.execute_on_collection(&coll, session).await;
                    // This test (in src/test/spec/json/sessions/server-support.json) runs two
                    // operations with implicit sessions in sequence and then checks to see if they
                    // used the same lsid. We delay for one second to ensure that the
                    // implicit session used in the first operation is returned to the pool before
                    // the second operation is executed.
                    if test.description == "Server supports implicit sessions" {
                        runtime::delay_for(Duration::from_secs(1)).await;
                    }
                    result
                }
                Some(OperationObject::Database) => {
                    operation.execute_on_database(&db, session).await
                }
                Some(OperationObject::Client) => operation.execute_on_client(&client).await,
                Some(OperationObject::Session0) => {
                    if operation.name == "endSession" {
                        let session = session0.take();
                        drop(session);
                        runtime::delay_for(Duration::from_secs(1)).await;
                        continue;
                    } else {
                        operation
                            .execute_on_session(session0.as_mut().unwrap())
                            .await
                    }
                }
                Some(OperationObject::Session1) => {
                    if operation.name == "endSession" {
                        let session = session1.take();
                        drop(session);
                        runtime::delay_for(Duration::from_secs(1)).await;
                        continue;
                    } else {
                        operation
                            .execute_on_session(session1.as_mut().unwrap())
                            .await
                    }
                }
                Some(OperationObject::TestRunner) => {
                    match operation.name.as_str() {
                        "assertDifferentLsidOnLastTwoCommands" => {
                            assert_different_lsid_on_last_two_commands(&client)
                        }
                        "assertSameLsidOnLastTwoCommands" => {
                            assert_same_lsid_on_last_two_commands(&client)
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
                            operation.execute_on_client(&internal_client).await.unwrap();
                        }
                        "targetedFailPoint" => {
                            let fail_point = from_bson(
                                operation
                                    .execute_on_client(&internal_client)
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

                            fail_point_guards.push(
                                client
                                    .deref()
                                    .enable_failpoint(fail_point, Some(selection_criteria))
                                    .await
                                    .unwrap(),
                            );
                        }
                        other => panic!("unknown operation: {}", other),
                    }
                    continue;
                }
                Some(OperationObject::GridfsBucket) => {
                    panic!("unsupported operation: {}", operation.name)
                }
            };

            if operation.error.is_none() && operation.result.is_none() && result.is_err() {
                log_uncaptured(format!(
                    "Ignoring operation error: {}",
                    result.clone().unwrap_err()
                ));
            }

            if let Some(error) = operation.error {
                assert_eq!(error, result.is_err(), "{}", &test.description);
            }

            if let Some(expected_result) = operation.result {
                match expected_result {
                    OperationResult::Success(expected) => {
                        let result = result.unwrap().unwrap();
                        assert_matches(&result, &expected, Some(&test.description));
                    }
                    OperationResult::Error(operation_error) => {
                        let error = result.unwrap_err();
                        if let Some(error_contains) = operation_error.error_contains {
                            let message = error.message().unwrap();
                            assert!(message.contains(&error_contains));
                        }
                        if let Some(error_code_name) = operation_error.error_code_name {
                            let code_name = error.code_name().unwrap();
                            assert_eq!(
                                error_code_name, code_name,
                                "{}: expected error with codeName {:?}, instead got {:#?}",
                                test.description, error_code_name, error
                            );
                        }
                        if let Some(error_code) = operation_error.error_code {
                            let code = error.code().unwrap();
                            assert_eq!(error_code, code);
                        }
                        if let Some(error_labels_contain) = operation_error.error_labels_contain {
                            let labels = error.labels();
                            error_labels_contain
                                .iter()
                                .for_each(|label| assert!(labels.contains(label)));
                        }
                        if let Some(error_labels_omit) = operation_error.error_labels_omit {
                            let labels = error.labels();
                            error_labels_omit
                                .iter()
                                .for_each(|label| assert!(!labels.contains(label)));
                        }
                        #[cfg(feature = "in-use-encryption-unstable")]
                        if let Some(t) = operation_error.is_timeout_error {
                            assert_eq!(
                                t,
                                error.is_network_timeout() || error.is_non_timeout_network_error()
                            )
                        }
                    }
                }
            }
        }

        drop(session0);
        drop(session1);

        // wait for the transaction in progress to be aborted implicitly when the session is dropped
        if test.description.as_str() == "implicit abort" {
            runtime::delay_for(Duration::from_secs(1)).await;
        }

        if let Some(expectations) = test.expectations {
            let events: Vec<CommandStartedEvent> = client
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

        if let Some(outcome) = test.outcome {
            outcome
                .assert_matches_actual(
                    db_name,
                    coll_name,
                    if is_csfle_test {
                        &internal_client
                    } else {
                        &client
                    },
                )
                .await;
        }
    }
}

fn majority_write_concern() -> WriteConcern {
    WriteConcern::builder().w(Acknowledgment::Majority).build()
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
