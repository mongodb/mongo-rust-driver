#[cfg(not(feature = "sync"))]
mod atlas_connectivity;
mod auth_aws;
mod client;
mod coll;
mod cursor;
mod db;
#[cfg(not(feature = "sync"))]
mod documentation_examples;
mod spec;
mod util;

pub(crate) use self::{
    spec::{run_spec_test, RunOn, Topology},
    util::{
        assert_matches,
        CmapEvent,
        CommandEvent,
        Event,
        EventClient,
        EventHandler,
        FailCommandOptions,
        FailPoint,
        FailPointMode,
        Matchable,
        TestClient,
    },
};

use lazy_static::lazy_static;

use self::util::TestLock;
use crate::{
    client::options::{ServerApi, ServerApiVersion},
    options::ClientOptions,
};
use std::str::FromStr;

const MAX_POOL_SIZE: u32 = 100;

lazy_static! {
    pub(crate) static ref CLIENT_OPTIONS: ClientOptions = {
        let uri = DEFAULT_URI.clone();
        let mut options = ClientOptions::parse_without_srv_resolution(&uri).unwrap();
        options.max_pool_size = Some(MAX_POOL_SIZE);
        options.server_api = SERVER_API.clone();

        options
    };
    pub(crate) static ref LOCK: TestLock = TestLock::new();
    pub(crate) static ref DEFAULT_URI: String =
        std::env::var("MONGODB_URI").unwrap_or_else(|_| "mongodb://localhost:27017".to_string());
    pub(crate) static ref SERVER_API: Option<ServerApi> = match std::env::var("MONGODB_API_VERSION")
    {
        Ok(server_api_version) => Some(ServerApi {
            version: ServerApiVersion::from_str(server_api_version.as_str()).unwrap(),
            deprecation_errors: None,
            strict: None,
        }),
        _ => None,
    };
}
