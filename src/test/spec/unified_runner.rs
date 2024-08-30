pub(crate) mod entity;
pub(crate) mod matcher;
pub(crate) mod operation;
pub(crate) mod test_event;
pub(crate) mod test_file;
pub(crate) mod test_runner;

use std::future::IntoFuture;

use futures::future::{BoxFuture, FutureExt};
use serde::Deserialize;

use crate::test::{file_level_log, log_uncaptured, spec::deserialize_spec_tests};

pub(crate) use self::{
    entity::{ClientEntity, Entity, SessionEntity, TestCursor},
    matcher::{events_match, results_match},
    operation::Operation,
    test_event::{ExpectedCmapEvent, ExpectedCommandEvent, ExpectedEvent, ObserveEvent},
    test_file::{
        merge_uri_options,
        CollectionData,
        ExpectError,
        ExpectedEventType,
        TestFile,
        TestFileEntity,
        Topology,
    },
    test_runner::{EntityMap, TestRunner},
};

pub(crate) fn run_unified_tests(spec: &'static [&'static str]) -> RunUnifiedTestsAction {
    RunUnifiedTestsAction {
        spec,
        skipped_files: None,
        skipped_tests: None,
        file_transformation: None,
    }
}

type FileTransformation = Box<dyn Fn(&mut TestFile) + Send + Sync>;
pub(crate) struct RunUnifiedTestsAction {
    spec: &'static [&'static str],
    skipped_files: Option<Vec<&'static str>>,
    skipped_tests: Option<Vec<&'static str>>,
    file_transformation: Option<FileTransformation>,
}

impl RunUnifiedTestsAction {
    /// The files to skip deserializing. The provided filenames should only contain the filename and
    /// extension, e.g. "unacknowledged-writes.json". Filenames are matched case-sensitively.
    pub(crate) fn skip_files(self, skipped_files: &[&'static str]) -> Self {
        Self {
            skipped_files: Some(skipped_files.to_vec()),
            ..self
        }
    }

    /// The descriptions of the tests to skip. Test descriptions are matched case-sensitively.
    pub(crate) fn skip_tests(self, skipped_tests: &[&'static str]) -> Self {
        Self {
            skipped_tests: Some(skipped_tests.to_vec()),
            ..self
        }
    }

    /// A transformation to apply to each test file prior to running the tests.
    pub(crate) fn transform_files(
        self,
        file_transformation: impl Fn(&mut TestFile) + Send + Sync + 'static,
    ) -> Self {
        Self {
            file_transformation: Some(Box::new(file_transformation)),
            ..self
        }
    }
}

impl IntoFuture for RunUnifiedTestsAction {
    type Output = ();
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            for (mut test_file, path) in
                deserialize_spec_tests::<TestFile>(self.spec, self.skipped_files.as_deref())
            {
                if let Some(ref file_transformation) = self.file_transformation {
                    file_transformation(&mut test_file);
                }

                let test_runner = TestRunner::new().await;
                test_runner
                    .run_test(test_file, path, self.skipped_tests.as_ref())
                    .await;
            }
        }
        .boxed()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn valid_pass() {
    let mut skipped_files = vec![
        // TODO RUST-1570: unskip this file
        "collectionData-createOptions.json",
        // TODO RUST-1405: unskip this file
        "expectedError-errorResponse.json",
        // TODO RUST-582: unskip these files
        "entity-cursor-iterateOnce.json",
        "matches-lte-operator.json",
        // TODO: unskip this file when the convenient transactions API tests are converted to the
        // unified format
        "poc-transactions-convenient-api.json",
    ];
    // These tests need the in-use-encryption feature flag to be deserialized and run.
    if cfg!(not(feature = "in-use-encryption")) {
        skipped_files.extend(&[
            "kmsProviders-placeholder_kms_credentials.json",
            "kmsProviders-unconfigured_kms.json",
            "kmsProviders-explicit_kms_credentials.json",
            "kmsProviders-mixed_kms_credential_fields.json",
        ]);
    }

    run_unified_tests(&["unified-test-format", "valid-pass"])
        .skip_files(&skipped_files)
        // This test relies on old OP_QUERY behavior that many drivers still use for < 4.4, but
        // we do not use, due to never implementing OP_QUERY.
        .skip_tests(&["A successful find event with a getmore and the server kills the cursor (<= 4.4)"])
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn valid_fail() {
    expect_failures(&["unified-test-format", "valid-fail"], None).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid() {
    expect_failures(
        &["unified-test-format", "invalid"],
        // We don't do the schema validation required by these tests to avoid lengthy
        // deserialization implementations.
        Some(&[
            "runOnRequirement-minProperties.json",
            "storeEventsAsEntity-events-enum.json",
            "tests-minItems.json",
            "expectedError-isError-const.json",
            "expectedError-minProperties.json",
            "storeEventsAsEntity-events-minItems.json",
            "expectedLogMessage-component-enum.json",
            "entity-client-observeLogMessages-minProperties.json",
            "test-expectLogMessages-minItems.json",
        ]),
    )
    .await;
}

#[derive(Debug)]
enum TestFileResult {
    Ok(TestFile),
    Err,
}

impl<'de> Deserialize<'de> for TestFileResult {
    // This implementation should always return Ok to avoid panicking during deserialization in
    // deserialize_spec_tests.
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Test files should always be valid JSON.
        let value = serde_json::Value::deserialize(deserializer).unwrap();
        match serde_json::from_value(value) {
            Ok(test_file) => Ok(TestFileResult::Ok(test_file)),
            Err(_) => Ok(TestFileResult::Err),
        }
    }
}

// The test runner enforces the unified test format schema during both deserialization and
// execution. Note that these tests may not fail as expected if the TEST_DESCRIPTION environment
// variable for skipping tests is set.
async fn expect_failures(spec: &[&str], skipped_files: Option<&'static [&'static str]>) {
    for (test_file_result, path) in deserialize_spec_tests::<TestFileResult>(spec, skipped_files) {
        log_uncaptured(format!("Expecting failure for {:?}", path));
        // If the test deserialized properly, then expect an error to occur during execution.
        if let TestFileResult::Ok(test_file) = test_file_result {
            std::panic::AssertUnwindSafe(async {
                TestRunner::new()
                    .await
                    .run_test(test_file, path.clone(), None)
                    .await;
            })
            .catch_unwind()
            .await
            .expect_err(&format!("Tests from {:?} should have failed", &path));
        }
    }
}
