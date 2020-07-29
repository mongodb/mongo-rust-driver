mod atlas_connectivity;
mod auth_aws;
mod client;
mod coll;
mod cursor;
mod db;
mod spec;
mod util;

pub(crate) use self::{
    spec::{run_spec_test, run_v2_test, AnyTestOperation, OperationObject, TestEvent},
    util::{assert_matches, CommandEvent, EventClient, Matchable, TestClient},
};

use lazy_static::lazy_static;

use self::util::TestLock;
use crate::options::ClientOptions;

const MAX_POOL_SIZE: u32 = 100;

lazy_static! {
    pub(crate) static ref CLIENT_OPTIONS: ClientOptions = {
        let uri = std::env::var("MONGODB_URI")
            .unwrap_or_else(|_| "mongodb://localhost:27017".to_string());
        let mut options = ClientOptions::parse_without_srv_resolution(&uri).unwrap();
        options.max_pool_size = Some(MAX_POOL_SIZE);

        options
    };
    pub(crate) static ref LOCK: TestLock = TestLock::new();
}
