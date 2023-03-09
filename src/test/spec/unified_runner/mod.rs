pub(crate) mod entity;
pub(crate) mod matcher;
pub(crate) mod observer;
pub(crate) mod operation;
pub(crate) mod test_event;
pub(crate) mod test_file;
pub(crate) mod test_runner;

use std::future::IntoFuture;

use futures::future::{BoxFuture, FutureExt};
use serde::Deserialize;
use tokio::sync::RwLockWriteGuard;

use crate::test::{file_level_log, spec::deserialize_spec_tests, LOCK};

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
    skipped_files: Option<&'static [&'static str]>,
    skipped_tests: Option<&'static [&'static str]>,
    file_transformation: Option<FileTransformation>,
}

impl RunUnifiedTestsAction {
    /// The files to skip deserializing. The provided filenames should only contain the filename and
    /// extension, e.g. "unacknowledged-writes.json". Filenames are matched case-sensitively.
    pub(crate) fn skip_files(self, skipped_files: &'static [&'static str]) -> Self {
        Self {
            spec: self.spec,
            skipped_files: Some(skipped_files),
            skipped_tests: self.skipped_tests,
            file_transformation: self.file_transformation,
        }
    }

    /// The descriptions of the tests to skip. Test descriptions are matched case-sensitively.
    pub(crate) fn skip_tests(self, skipped_tests: &'static [&'static str]) -> Self {
        Self {
            spec: self.spec,
            skipped_files: self.skipped_files,
            skipped_tests: Some(skipped_tests),
            file_transformation: self.file_transformation,
        }
    }

    /// A transformation to apply to each test file prior to running the tests.
    pub(crate) fn transform_files(
        self,
        file_transformation: impl Fn(&mut TestFile) + Send + Sync + 'static,
    ) -> Self {
        Self {
            spec: self.spec,
            skipped_files: self.skipped_files,
            skipped_tests: self.skipped_tests,
            file_transformation: Some(Box::new(file_transformation)),
        }
    }
}

impl IntoFuture for RunUnifiedTestsAction {
    type Output = ();
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            for (mut test_file, path) in
                deserialize_spec_tests::<TestFile>(self.spec, self.skipped_files)
            {
                if let Some(ref file_transformation) = self.file_transformation {
                    file_transformation(&mut test_file);
                }

                let test_runner = TestRunner::new().await;
                test_runner
                    .run_test(test_file, path, self.skipped_tests)
                    .await;
            }
        }
        .boxed()
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn test_examples() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;
    run_unified_tests(&["unified-test-format", "examples"]).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn valid_pass() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;
    run_unified_tests(&["unified-test-format", "valid-pass"]).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn valid_fail() {
    expect_failures(&["unified-test-format", "valid-fail"]).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn invalid() {
    expect_failures(&["unified-test-format", "invalid"]).await;
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

/// The test runner enforces the unified test format schema during both deserialization and
/// execution.
async fn expect_failures(spec: &[&str]) {
    for (test_file_result, path) in deserialize_spec_tests::<TestFileResult>(spec, None) {
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
