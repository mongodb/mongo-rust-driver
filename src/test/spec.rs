mod auth;
mod change_streams;
#[cfg(feature = "in-use-encryption-unstable")]
mod client_side_encryption;
mod collection_management;
mod command_monitoring;
mod connection_stepdown;
mod crud;
mod faas;
mod gridfs;
mod handshake;
mod index_management;
#[cfg(feature = "dns-resolver")]
mod initial_dns_seedlist_discovery;
mod load_balancers;
mod ocsp;
mod oidc;
mod read_write_concern;
mod retryable_reads;
mod retryable_writes;
mod run_command;
mod sdam;
mod sessions;
#[cfg(feature = "tracing-unstable")]
mod trace;
mod transactions;
pub(crate) mod unified_runner;
mod v2_runner;
mod versioned_api;
mod write_error;

use std::{
    any::type_name,
    ffi::OsStr,
    fs::{read_dir, File},
    future::Future,
    path::PathBuf,
};

use serde::{de::DeserializeOwned, Deserialize};

pub(crate) use self::{
    unified_runner::{merge_uri_options, ExpectedEventType, Topology},
    v2_runner::{operation::Operation, test_file::RunOn},
};
use crate::{bson::Bson, test::SERVERLESS};

use super::log_uncaptured;

pub(crate) fn deserialize_spec_tests<T: DeserializeOwned>(
    spec: &[&str],
    skipped_files: Option<&[&str]>,
) -> Vec<(T, PathBuf)> {
    let dir_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "src", "test", "spec", "json"]
        .iter()
        .chain(spec.iter())
        .collect();

    let mut tests = vec![];
    for entry in read_dir(&dir_path)
        .unwrap_or_else(|e| panic!("Failed to read directory at {:?}: {}", &dir_path, e))
    {
        let path = entry.unwrap().path();
        let Some(filename) = path
            .file_name()
            .and_then(OsStr::to_str)
            .filter(|name| name.ends_with(".json"))
        else {
            continue;
        };

        if let Ok(unskipped_filename) = std::env::var("TEST_FILE") {
            if filename != unskipped_filename {
                continue;
            }
        }

        if let Some(skipped_files) = skipped_files {
            if skipped_files.contains(&filename) {
                log_uncaptured(format!("Skipping deserializing {:?}", &path));
                continue;
            }
        }

        let file = File::open(&path)
            .unwrap_or_else(|e| panic!("Failed to open file at {:?}: {}", &path, e));

        // Use BSON as an intermediary to deserialize extended JSON properly.
        let deserializer = &mut serde_json::Deserializer::from_reader(file);
        let test_bson: Bson = serde_path_to_error::deserialize(deserializer).unwrap_or_else(|e| {
            panic!(
                "Failed to deserialize test JSON to BSON in {:?}: {}",
                &path, e
            )
        });

        let deserializer = bson::Deserializer::new(test_bson);
        let test: T = serde_path_to_error::deserialize(deserializer).unwrap_or_else(|e| {
            panic!(
                "Failed to deserialize test BSON to {} in {:?}: {}",
                type_name::<T>(),
                &path,
                e
            )
        });

        tests.push((test, path));
    }

    tests
}

pub(crate) async fn run_spec_test<T, F, G>(spec: &[&str], run_test_file: F)
where
    F: Fn(T) -> G,
    G: Future<Output = ()>,
    T: DeserializeOwned,
{
    for (test_file, _) in deserialize_spec_tests(spec, None) {
        run_test_file(test_file).await;
    }
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
