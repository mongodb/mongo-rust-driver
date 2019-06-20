#[macro_use]
extern crate approx;
#[macro_use]
extern crate bson;
#[macro_use]
extern crate function_name;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;

mod client;
mod coll;
mod db;
mod spec;

use std::collections::HashMap;

use bson::{oid::ObjectId, Bson};
use mongodb::{
    concern::{Acknowledgment, ReadConcern, WriteConcern},
    options::ClientOptions,
    read_preference::ReadPreference,
    Client, Collection, Database,
};

lazy_static! {
    static ref CLIENT: Client = {
        let uri = option_env!("MONGODB_URI").unwrap_or("mongodb://localhost:27017");
        let mut options = ClientOptions::parse(uri).unwrap();
        options.max_pool_size = Some(100);

        if options.repl_set_name.is_some() || options.hosts.len() > 1 {
            options.read_preference = Some(ReadPreference::Primary);
            options.read_concern = Some(ReadConcern::Linearizable);
            options.write_concern =
                Some(WriteConcern::builder().w(Acknowledgment::Majority).build());
        }

        Client::with_options(options).unwrap()
    };
    static ref SERVER_INFO: IsMasterCommandResponse = {
        bson::from_bson(Bson::Document(
            CLIENT
                .database("admin")
                .run_command(doc! { "isMaster":  1 }, None)
                .unwrap(),
        ))
        .unwrap()
    };
}

#[allow(dead_code)]
fn version_at_least_40() -> bool {
    SERVER_INFO
        .max_wire_version
        .map(|v| v >= 7)
        .unwrap_or(false)
}

fn get_db(db_name: &str) -> Database {
    CLIENT.database(db_name)
}

fn get_coll(db_name: &str, coll_name: &str) -> Collection {
    get_db(db_name).collection(coll_name)
}

fn init_db_and_coll(db_name: &str, coll_name: &str) -> Collection {
    let coll = get_coll(db_name, coll_name);
    coll.drop().unwrap();
    coll
}

// Copy of the internal isMaster struct; fix this later.
#[derive(Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IsMasterCommandResponse {
    #[serde(rename = "ismaster")]
    pub is_master: Option<bool>,
    pub ok: Option<f32>,
    pub hosts: Option<Vec<String>>,
    pub passives: Option<Vec<String>>,
    pub arbiters: Option<Vec<String>>,
    pub msg: Option<String>,
    pub me: Option<String>,
    pub set_version: Option<i32>,
    pub set_name: Option<String>,
    pub hidden: Option<bool>,
    pub secondary: Option<bool>,
    pub arbiter_only: Option<bool>,
    #[serde(rename = "isreplicaset")]
    pub is_replica_set: Option<bool>,
    pub logical_session_timeout_minutes: Option<i64>,
    pub min_wire_version: Option<i32>,
    pub max_wire_version: Option<i32>,
    pub tags: Option<HashMap<String, String>>,
    pub election_id: Option<ObjectId>,
    pub primary: Option<String>,
}
