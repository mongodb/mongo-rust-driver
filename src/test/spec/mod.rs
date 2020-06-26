mod auth;
mod command_monitoring;
mod connection_stepdown;
mod crud_v1;
mod crud_v2;
mod initial_dns_seedlist_discovery;
mod read_write_concern;
mod retryable_reads;
mod runner;

use std::{
    convert::TryFrom,
    ffi::OsStr,
    fs::{self, File},
    future::Future,
    path::PathBuf,
};

pub use self::runner::{
    run_v2_test,
    AnyTestOperation,
    OperationObject,
    TestData,
    TestEvent,
    TestFile,
};

use serde::Deserialize;
use serde_json::Value;

use crate::bson::Bson;

pub(crate) async fn run_spec_test<'a, T, F, G>(spec: &[&str], run_test_file: F)
where
    F: Fn(T) -> G,
    G: Future<Output = ()>,
    T: Deserialize<'a>,
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
        let json: Value =
            serde_json::from_reader(File::open(test_file_full_path.as_path()).unwrap()).unwrap();

        run_test_file(
            bson::from_bson(
                Bson::try_from(json)
                    .unwrap_or_else(|_| panic!(test_file_full_path.display().to_string())),
            )
            .unwrap() // .unwrap_or_else(|_| panic!(test_file_full_path.display().to_string())),
        )
        .await
    }
}
