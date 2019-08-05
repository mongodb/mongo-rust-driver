extern crate serde;
extern crate serde_json;

use std::{
    ffi::OsStr,
    fs::{self, File},
    path::PathBuf,
};

use bson::Bson;
use semver::Version;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    concern::{Acknowledgment, ReadConcern, WriteConcern},
    options::ClientOptions,
    read_preference::ReadPreference,
    Client,
};

lazy_static! {
    pub static ref CLIENT_OPTIONS: ClientOptions = {
        let uri = option_env!("MONGODB_URI").unwrap_or("mongodb://localhost:27017");
        let mut options = ClientOptions::parse(uri).unwrap();
        options.max_pool_size = Some(100);

        if options.repl_set_name.is_some() || options.hosts.len() > 1 {
            options.read_preference = Some(ReadPreference::Primary);
            options.read_concern = Some(ReadConcern::Linearizable);
            options.write_concern =
                Some(WriteConcern::builder().w(Acknowledgment::Majority).build());
        }

        options
    };
}

pub fn run<'a, T, F>(spec: &[&str], run_test_file: F)
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

        println!("file: {}", test_file_path.display());

        let test_file_full_path = base_path.join(&test_file_path);
        let json: Value =
            serde_json::from_reader(File::open(test_file_full_path.as_path()).unwrap()).unwrap();

        run_test_file(bson::from_bson(Bson::from(json)).unwrap())
    }
}

#[derive(Debug, Deserialize)]
struct ServerStatus {
    version: String,
}

pub fn server_version_at_least(client: &Client, min_version: &str) -> bool {
    let min_version = Version::parse(min_version).unwrap();

    let server_status: ServerStatus = bson::from_bson(Bson::Document(
        client
            .database("admin")
            .run_command(doc! { "serverStatus" : 1 }, Some(ReadPreference::Primary))
            .unwrap(),
    ))
    .unwrap();

    let server_version = Version::parse(&server_status.version).unwrap();
    server_version >= min_version
}
