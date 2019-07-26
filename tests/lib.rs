#![recursion_limit = "128"]

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
mod util;

use util::TestClient;

lazy_static! {
    static ref CLIENT: TestClient = TestClient::new();
}
