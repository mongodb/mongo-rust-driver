#![recursion_limit = "128"]

mod client;
mod coll;
mod db;
pub(crate) mod spec;
mod util;

use lazy_static::lazy_static;

use crate::util::TestClient;

lazy_static! {
    static ref CLIENT: TestClient = TestClient::new();
}
