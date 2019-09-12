use std::{
    ffi::OsStr,
    fs::{self, File},
    path::PathBuf,
};

use bson::Bson;
use serde::Deserialize;
use serde_json::Value;

pub fn run<'a, T, F>(spec: &[&str], run_test_file: F)
where
    F: Fn(T),
    T: Deserialize<'a>,
{
    let base_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "spec", "json"]
        .iter()
        .chain(spec.iter())
        .collect();

    let mut entries: Vec<_> = fs::read_dir(&base_path)
        .unwrap()
        .map(Result::unwrap)
        .collect();
    entries.sort_unstable_by_key(|entry| entry.path());

    for entry in entries {
        if !entry.file_type().unwrap().is_file() {
            continue;
        }

        let test_file_path = PathBuf::from(entry.file_name());
        if test_file_path.extension().and_then(OsStr::to_str) != Some("json") {
            continue;
        }

        dbg!(&test_file_path);

        let test_file_full_path = base_path.join(&test_file_path);
        let json: Value =
            serde_json::from_reader(File::open(test_file_full_path.as_path()).unwrap()).unwrap();

        run_test_file(bson::from_bson(Bson::from(json)).unwrap())
    }
}
