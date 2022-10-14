pub(crate) mod entity;
pub(crate) mod matcher;
pub(crate) mod observer;
pub(crate) mod operation;
pub(crate) mod test_event;
pub(crate) mod test_file;
pub(crate) mod test_runner;

use std::{convert::TryFrom, ffi::OsStr, fs::read_dir, path::PathBuf};

use futures::future::FutureExt;
use semver::Version;
use tokio::sync::RwLockWriteGuard;

use crate::test::{log_uncaptured, run_single_test, LOCK};

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
        TestCase,
        TestFile,
        TestFileEntity,
        Topology,
    },
    test_runner::{EntityMap, TestRunner},
};

use super::run_spec_test_with_path;

static MIN_SPEC_VERSION: Version = Version::new(1, 0, 0);
static MAX_SPEC_VERSION: Version = Version::new(1, 13, 0);

fn file_level_log(message: impl AsRef<str>) {
    log_uncaptured(format!("\n------------\n{}\n", message.as_ref()));
}

pub(crate) async fn run_unified_format_test(path: PathBuf, test_file: TestFile) {
    run_unified_format_test_filtered(path, test_file, |_| true).await
}

pub(crate) async fn run_unified_format_test_filtered(
    path: PathBuf,
    test_file: TestFile,
    pred: impl Fn(&TestCase) -> bool,
) {
    assert!(
        test_file.schema_version >= MIN_SPEC_VERSION
            && test_file.schema_version <= MAX_SPEC_VERSION,
        "Test runner not compatible with specification version {}",
        &test_file.schema_version
    );

    let test_runner = TestRunner::new().await;
    test_runner.run_test(path, test_file, pred).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn test_examples() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;
    run_spec_test_with_path(
        &["unified-test-format", "examples"],
        run_unified_format_test,
    )
    .await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn valid_fail() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let path: PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "src",
        "test",
        "spec",
        "json",
        "unified-test-format",
        "valid-fail",
    ]
    .iter()
    .collect();

    for entry in read_dir(&path).unwrap() {
        let test_file_path = PathBuf::from(entry.unwrap().file_name());
        let path = path.join(&test_file_path);
        let path_display = path.display().to_string();

        std::panic::AssertUnwindSafe(run_single_test(path, &run_unified_format_test))
            .catch_unwind()
            .await
            .expect_err(&format!("tests from {} should have failed", path_display));
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn valid_pass() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;
    run_spec_test_with_path(
        &["unified-test-format", "valid-pass"],
        run_unified_format_test,
    )
    .await;
}

const SKIPPED_INVALID_TESTS: &[&str] = &[
    // Event types are validated at test execution time, not parse time.
    "expectedEventsForClient-events_conflicts_with_cmap_eventType.json",
    "expectedEventsForClient-events_conflicts_with_command_eventType.json",
    "expectedEventsForClient-events_conflicts_with_default_eventType.json",
];

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn invalid() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let path: PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "src",
        "test",
        "spec",
        "json",
        "unified-test-format",
        "invalid",
    ]
    .iter()
    .collect();

    for entry in read_dir(&path).unwrap() {
        let test_file = entry.unwrap();
        if !test_file.file_type().unwrap().is_file() {
            continue;
        }
        let test_file_path = PathBuf::from(test_file.file_name());
        if test_file_path.extension().and_then(OsStr::to_str) != Some("json") {
            continue;
        }
        let test_file_str = test_file_path.as_os_str().to_str().unwrap();
        if SKIPPED_INVALID_TESTS
            .iter()
            .any(|skip| *skip == test_file_str)
        {
            file_level_log(format!("Skipping {}", test_file_str));
            continue;
        }
        let path = path.join(&test_file_path);
        let path_display = path.display().to_string();

        file_level_log(format!("Attempting to parse {}", path_display));

        let json: serde_json::Value =
            serde_json::from_reader(std::fs::File::open(path.as_path()).unwrap()).unwrap();
        let result: Result<TestFile, _> = bson::from_bson(
            bson::Bson::try_from(json).unwrap_or_else(|_| panic!("{}", path_display)),
        );
        if let Ok(test_file) = result {
            panic!(
                "{}: should be invalid, parsed to:\n{:#?}",
                path_display, test_file
            );
        }
    }
}
