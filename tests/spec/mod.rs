mod connection_stepdown;

use std::{
    ffi::OsStr,
    fs::{self, File},
    path::PathBuf,
};

use bson::Bson;
use serde::Deserialize;
use serde_json::Value;

fn test<'a, T, F>(spec: &[&str], run_test_file: F)
where
    F: Fn(T),
    T: Deserialize<'a>,
{
    let base_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "spec", "json"]
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
