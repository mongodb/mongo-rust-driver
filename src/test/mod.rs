mod atlas_connectivity;
mod client;
mod coll;
mod cursor;
mod db;
mod spec;
mod util;

pub(crate) use self::{
    spec::{run_spec_test, AnyTestOperation, OperationObject, TestEvent, TestFile, TestData},
    util::{assert_matches, parse_version, CommandEvent, EventClient, Matchable, TestClient},
};

use lazy_static::lazy_static;

use self::util::TestLock;
use crate::{options::ClientOptions, RUNTIME};

const MAX_POOL_SIZE: u32 = 100;

lazy_static! {
    pub(crate) static ref CLIENT_OPTIONS: ClientOptions = {
        let uri = std::env::var("MONGODB_URI")
            .unwrap_or_else(|_| "mongodb://localhost:27017".to_string());
        let mut options = RUNTIME.block_on(ClientOptions::parse(&uri)).unwrap();
        options.max_pool_size = Some(MAX_POOL_SIZE);

        options
    };
    pub(crate) static ref LOCK: TestLock = TestLock::new();
}
