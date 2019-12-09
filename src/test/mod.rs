mod atlas_connectivity;
mod client;
mod coll;
mod db;
mod spec;
mod util;

pub(crate) use self::spec::run_spec_test;

use lazy_static::lazy_static;

use self::util::{TestClient, TestLock};
use crate::options::ClientOptions;

lazy_static! {
    pub(crate) static ref CLIENT: TestClient = TestClient::new();
    pub(crate) static ref CLIENT_OPTIONS: ClientOptions = CLIENT.options.clone();
    pub(crate) static ref LOCK: TestLock = TestLock::new();
}
