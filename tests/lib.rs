#[macro_use]
extern crate approx;
#[macro_use]
extern crate bson;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;

mod client;
mod coll;
mod db;
mod spec;

use mongodb::{Client, Collection};

lazy_static! {
    static ref MONGODB_URI: String =
        {
            let mut uri = option_env!("MONGODB_URI").unwrap_or("mongodb://localhost:27017").to_string();

            // Kind of a hack; we should probably fix this at some point.
            if uri.contains("replicaSet=") {
                uri.push_str("&readPreference=primaryPreferred&w=majority&readConcernLevel=linearizable");
            }

            uri
        };
    static ref CLIENT: Client = Client::with_uri(MONGODB_URI.as_str()).unwrap();
}

fn get_coll(db_name: &str, coll_name: &str) -> Collection {
    CLIENT.database(db_name).collection(coll_name)
}

fn init_db_and_coll(db_name: &str, coll_name: &str) -> Collection {
    let coll = get_coll(db_name, coll_name);
    coll.drop().unwrap();
    coll
}
