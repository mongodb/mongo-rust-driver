#![recursion_limit = "128"]

mod client;
mod coll;
mod db;
pub(crate) mod spec;
mod util;

use lazy_static::lazy_static;

use crate::util::{TestClient, TestLock};

lazy_static! {
    static ref CLIENT: TestClient = TestClient::new();
    static ref LOCK: TestLock = TestLock::new();
}
