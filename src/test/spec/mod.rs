mod auth;
mod command_monitoring;
mod connection_stepdown;
mod crud;
mod initial_dns_seedlist_discovery;
mod read_write_concern;

use std::{
    ffi::OsStr,
    fs::{self, File},
    path::PathBuf,
};

use bson::Bson;
use serde::Deserialize;
use serde_json::Value;

pub(crate) fn run_spec_test<'a, T, F>(spec: &[&str], run_test_file: F)
where
    F: Fn(T),
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

        run_test_file(bson::from_bson(Bson::from(json)).unwrap())
    }
}

pub(crate) fn load_test<'a, T>(spec: &[&str]) -> T
where
    T: Deserialize<'a>,
{
    let test_file_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "src", "test", "spec", "json"]
        .iter()
        .chain(spec.iter())
        .collect();

    if test_file_path.extension().and_then(OsStr::to_str) != Some("json") {
        panic!("not a json file!");
    }

    let json: Value =
        serde_json::from_reader(File::open(test_file_path.as_path()).unwrap()).unwrap();

    bson::from_bson(Bson::from(json)).unwrap()
}
