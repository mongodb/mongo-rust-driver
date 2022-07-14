#[cfg(all(not(feature = "sync"), not(feature = "tokio-sync")))]
mod auth;
mod change_streams;
mod collection_management;
mod command_monitoring;
mod connection_stepdown;
mod crud;
mod crud_v1;
#[cfg(all(not(feature = "sync"), not(feature = "tokio-sync")))]
mod initial_dns_seedlist_discovery;
mod load_balancers;
mod ocsp;
#[cfg(all(not(feature = "sync"), not(feature = "tokio-sync")))]
mod read_write_concern;
mod retryable_reads;
mod retryable_writes;
mod sessions;
mod transactions;
pub mod unified_runner;
mod v2_runner;
mod versioned_api;
mod write_error;

use std::{
    convert::TryFrom,
    ffi::OsStr,
    fs::{self, File},
    future::Future,
    path::PathBuf,
};

pub(crate) use self::{
    unified_runner::{
        merge_uri_options,
        run_unified_format_test,
        run_unified_format_test_filtered,
        ExpectedEventType,
        Topology,
    },
    v2_runner::{operation::Operation, run_v2_test, test_file::RunOn},
};

use serde::{de::DeserializeOwned, Deserialize};
use serde_json::Value;

use crate::{bson::Bson, test::SERVERLESS};

pub(crate) async fn run_spec_test<T, F, G>(spec: &[&str], run_test_file: F)
where
    F: Fn(T) -> G,
    G: Future<Output = ()>,
    T: DeserializeOwned,
{
    run_spec_test_with_path(spec, |_, t| run_test_file(t)).await
}

pub(crate) async fn run_spec_test_with_path<T, F, G>(spec: &[&str], run_test_file: F)
where
    F: Fn(PathBuf, T) -> G,
    G: Future<Output = ()>,
    T: DeserializeOwned,
{
    let base_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "src", "test", "spec", "json"]
        .iter()
        .chain(spec.iter())
        .collect();

    for entry in fs::read_dir(&base_path).unwrap() {
        let test_file = entry.unwrap();

        if !test_file.file_type().unwrap().is_file() {
            continue;
        }

        let test_file_path = PathBuf::from(test_file.file_name());
        if test_file_path.extension().and_then(OsStr::to_str) != Some("json") {
            continue;
        }

        let test_file_full_path = base_path.join(&test_file_path);
        run_single_test_with_path(test_file_full_path, &run_test_file).await;
    }
}

pub(crate) async fn run_single_test<T, F, G>(path: PathBuf, run_test_file: &F)
where
    F: Fn(T) -> G,
    G: Future<Output = ()>,
    T: DeserializeOwned,
{
    run_single_test_with_path(path, &|_, t| run_test_file(t)).await
}

pub(crate) async fn run_single_test_with_path<T, F, G>(path: PathBuf, run_test_file: &F)
where
    F: Fn(PathBuf, T) -> G,
    G: Future<Output = ()>,
    T: DeserializeOwned,
{
    let json: Value = serde_json::from_reader(File::open(path.as_path()).unwrap()).unwrap();

    // Printing the name of the test file makes it easier to debug deserialization errors.
    println!("Running tests from {}", path.display());

    run_test_file(
        path.clone(),
        bson::from_bson(
            Bson::try_from(json).unwrap_or_else(|err| panic!("{}: {}", path.display(), err)),
        )
        .unwrap_or_else(|e| panic!("{}: {}", path.display(), e)),
    )
    .await
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase", deny_unknown_fields)]
pub(crate) enum Serverless {
    Require,
    Forbid,
    Allow,
}

impl Serverless {
    pub(crate) fn can_run(&self) -> bool {
        match self {
            Self::Forbid if *SERVERLESS => false,
            Self::Require if !*SERVERLESS => false,
            _ => true,
        }
    }
}
