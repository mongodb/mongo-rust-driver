#[cfg(all(not(feature = "sync"), not(feature = "tokio-sync")))]
mod atlas_connectivity;
mod auth_aws;
mod change_stream;
mod client;
mod coll;
mod cursor;
mod db;
#[cfg(all(not(feature = "sync"), not(feature = "tokio-sync")))]
mod documentation_examples;
mod index_management;
mod spec;
mod util;

pub(crate) use self::{
    spec::{run_single_test, run_spec_test, run_spec_test_with_path, RunOn, Serverless, Topology},
    util::{
        assert_matches,
        eq_matches,
        log_uncaptured,
        CmapEvent,
        CommandEvent,
        Event,
        EventClient,
        EventHandler,
        FailCommandOptions,
        FailPoint,
        FailPointMode,
        MatchErrExt,
        Matchable,
        SdamEvent,
        TestClient,
    },
};

use home::home_dir;
use lazy_static::lazy_static;

use self::util::TestLock;
use crate::{
    client::{
        auth::Credential,
        options::{ServerApi, ServerApiVersion},
    },
    options::{ClientOptions, Compressor},
};
use std::{fs::read_to_string, str::FromStr};

const MAX_POOL_SIZE: u32 = 100;

lazy_static! {
    pub(crate) static ref CLIENT_OPTIONS: ClientOptions = {
        let mut options = ClientOptions::parse_without_srv_resolution(&DEFAULT_URI).unwrap();
        update_options_for_testing(&mut options);
        options
    };
    pub(crate) static ref LOCK: TestLock = TestLock::new();
    pub(crate) static ref DEFAULT_URI: String = get_default_uri();
    pub(crate) static ref SERVER_API: Option<ServerApi> = match std::env::var("MONGODB_API_VERSION")
    {
        Ok(server_api_version) if !server_api_version.is_empty() => Some(ServerApi {
            version: ServerApiVersion::from_str(server_api_version.as_str()).unwrap(),
            deprecation_errors: None,
            strict: None,
        }),
        _ => None,
    };
    pub(crate) static ref SERVERLESS: bool =
        matches!(std::env::var("SERVERLESS"), Ok(s) if s == "serverless");
    pub(crate) static ref LOAD_BALANCED_SINGLE_URI: Option<String> =
        std::env::var("SINGLE_MONGOS_LB_URI").ok();
    pub(crate) static ref LOAD_BALANCED_MULTIPLE_URI: Option<String> =
        std::env::var("MULTI_MONGOS_LB_URI").ok();
    pub(crate) static ref ZSTD_COMPRESSION_ENABLED: bool =
        matches!(std::env::var("ZSTD_COMPRESSION_ENABLED"), Ok(s) if s == "true");
    pub(crate) static ref ZLIB_COMPRESSION_ENABLED: bool =
        matches!(std::env::var("ZLIB_COMPRESSION_ENABLED"), Ok(s) if s == "true");
    pub(crate) static ref SNAPPY_COMPRESSION_ENABLED: bool =
        matches!(std::env::var("SNAPPY_COMPRESSION_ENABLED"), Ok(s) if s == "true");
    pub(crate) static ref SERVERLESS_ATLAS_USER: Option<String> =
        std::env::var("SERVERLESS_ATLAS_USER").ok();
    pub(crate) static ref SERVERLESS_ATLAS_PASSWORD: Option<String> =
        std::env::var("SERVERLESS_ATLAS_PASSWORD").ok();
}

pub(crate) fn update_options_for_testing(options: &mut ClientOptions) {
    if options.max_pool_size.is_none() {
        options.max_pool_size = Some(MAX_POOL_SIZE);
    }
    if options.server_api.is_none() {
        options.server_api = SERVER_API.clone();
    }
    if options.compressors.is_none() {
        options.compressors = get_compressors();
    }
    if options.credential.is_none() && SERVERLESS_ATLAS_USER.is_some() {
        options.credential = Some(
            Credential::builder()
                .username(SERVERLESS_ATLAS_USER.clone())
                .password(SERVERLESS_ATLAS_PASSWORD.clone())
                .build(),
        );
    }
}

fn get_compressors() -> Option<Vec<Compressor>> {
    #[allow(unused_mut)]
    let mut compressors = vec![];

    if *SNAPPY_COMPRESSION_ENABLED {
        #[cfg(feature = "snappy-compression")]
        compressors.push(Compressor::Snappy);
        #[cfg(not(feature = "snappy-compression"))]
        panic!("To use snappy compression, the \"snappy-compression\" feature flag must be set.");
    }
    if *ZLIB_COMPRESSION_ENABLED {
        #[cfg(feature = "zlib-compression")]
        compressors.push(Compressor::Zlib { level: None });
        #[cfg(not(feature = "zlib-compression"))]
        panic!("To use zlib compression, the \"zlib-compression\" feature flag must be set.");
    }
    if *ZSTD_COMPRESSION_ENABLED {
        #[cfg(feature = "zstd-compression")]
        compressors.push(Compressor::Zstd { level: None });
        #[cfg(not(feature = "zstd-compression"))]
        panic!("To use zstd compression, the \"zstd-compression\" feature flag must be set.");
    }
    if compressors.is_empty() {
        None
    } else {
        Some(compressors)
    }
}

fn get_default_uri() -> String {
    if let Some(uri) = LOAD_BALANCED_SINGLE_URI.clone() {
        if !uri.is_empty() {
            return uri;
        }
    }
    if let Ok(uri) = std::env::var("MONGODB_URI") {
        return uri;
    }
    if let Some(mut home) = home_dir() {
        home.push(".mongodb_uri");
        if let Ok(uri) = read_to_string(home) {
            return uri;
        }
    }
    "mongodb://localhost:27017".to_string()
}
