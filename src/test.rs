#[cfg(all(not(feature = "sync"), not(feature = "tokio-sync")))]
mod atlas_connectivity;
mod atlas_planned_maintenance_testing;
#[cfg(feature = "aws-auth")]
mod auth_aws;
mod change_stream;
mod client;
mod coll;
#[cfg(feature = "in-use-encryption-unstable")]
mod csfle;
mod cursor;
mod db;
#[cfg(all(not(feature = "sync"), not(feature = "tokio-sync")))]
mod documentation_examples;
mod index_management;
#[cfg(all(not(feature = "sync"), not(feature = "tokio-sync")))]
mod lambda_examples;
pub(crate) mod spec;
mod timeseries;
pub(crate) mod util;

#[cfg(feature = "in-use-encryption-unstable")]
pub(crate) use self::csfle::{KmsProviderList, KMS_PROVIDERS_MAP};
pub(crate) use self::{
    spec::{run_spec_test, RunOn, Serverless, Topology},
    util::{
        assert_matches,
        eq_matches,
        file_level_log,
        log_uncaptured,
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
use tokio::sync::OnceCell;

#[cfg(feature = "tracing-unstable")]
use self::util::TracingHandler;
use crate::{
    client::{
        auth::Credential,
        options::{ServerApi, ServerApiVersion},
    },
    options::{ClientOptions, Compressor},
};
use std::{fs::read_to_string, str::FromStr};

static CLIENT_OPTIONS: OnceCell<ClientOptions> = OnceCell::const_new();
pub(crate) async fn get_client_options() -> &'static ClientOptions {
    CLIENT_OPTIONS
        .get_or_init(|| async {
            let mut options = ClientOptions::parse_uri(&*DEFAULT_URI, None).await.unwrap();
            update_options_for_testing(&mut options);
            options
        })
        .await
}

lazy_static! {
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

// conditional definitions do not work within the lazy_static! macro, so this
// needs to be defined separately.
#[cfg(feature = "tracing-unstable")]
lazy_static! {
    /// A global default tracing handler that will be installed the first time this
    /// value is accessed. A global handler must be used anytime the multi-threaded
    /// test runtime is in use, as non-global handlers only apply to the thread
    /// they are registered in.
    /// By default this handler will collect no tracing events.
    /// Its minimum severity levels can be configured on a per-component basis using
    /// [`TracingHandler:set_levels`]. The test lock MUST be acquired exclusively in
    /// any test that will use the handler to avoid mixing events from multiple tests.
    pub(crate) static ref DEFAULT_GLOBAL_TRACING_HANDLER: TracingHandler = {
        let handler = TracingHandler::new();
        tracing::subscriber::set_global_default(handler.clone())
            .expect("setting global default tracing subscriber failed");
        handler
    };
}

pub(crate) fn update_options_for_testing(options: &mut ClientOptions) {
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
