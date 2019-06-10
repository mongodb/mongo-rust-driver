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

        if options.repl_set_name.is_some() || options.hosts.len() > 1 {
            options.read_preference = Some(ReadPreference::Primary);
            options.read_concern = Some(ReadConcern::Linearizable);
            options.write_concern =
                Some(WriteConcern::builder().w(Acknowledgment::Majority).build());
        }

        Client::with_options(options).unwrap()
    };
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
