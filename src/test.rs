#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]

#[cfg(feature = "dns-resolver")]
mod atlas_connectivity;
mod atlas_planned_maintenance_testing;
#[cfg(feature = "aws-auth")]
mod auth_aws;
mod bulk_write;
mod change_stream;
mod client;
mod coll;
#[cfg(any(
    feature = "zstd-compression",
    feature = "zlib-compression",
    feature = "snappy-compression"
))]
mod compression;
#[cfg(feature = "in-use-encryption-unstable")]
mod csfle;
mod cursor;
mod db;
mod documentation_examples;
mod index_management;
mod lambda_examples;
pub(crate) mod spec;
mod timeseries;
pub(crate) mod util;

#[cfg(feature = "in-use-encryption-unstable")]
pub(crate) use self::csfle::{KmsProviderList, KMS_PROVIDERS_MAP};
#[allow(deprecated)]
pub(crate) use self::util::EventClient;
pub(crate) use self::{
    spec::{run_spec_test, RunOn, Serverless, Topology},
    util::{
        assert_matches,
        eq_matches,
        file_level_log,
        log_uncaptured,
        Event,
        MatchErrExt,
        Matchable,
        TestClient,
    },
};

use home::home_dir;
use once_cell::sync::Lazy;
use tokio::sync::OnceCell;

#[cfg(feature = "tracing-unstable")]
use self::util::TracingHandler;
use crate::{
    client::{
        auth::Credential,
        options::{ServerApi, ServerApiVersion},
    },
    options::ClientOptions,
};
use std::{fs::read_to_string, str::FromStr};

static CLIENT_OPTIONS: OnceCell<ClientOptions> = OnceCell::const_new();
pub(crate) async fn get_client_options() -> &'static ClientOptions {
    CLIENT_OPTIONS
        .get_or_init(|| async {
            let mut options = ClientOptions::parse(&*DEFAULT_URI).await.unwrap();
            update_options_for_testing(&mut options);
            options
        })
        .await
}

pub(crate) static DEFAULT_URI: Lazy<String> = Lazy::new(get_default_uri);
pub(crate) static SERVER_API: Lazy<Option<ServerApi>> =
    Lazy::new(|| match std::env::var("MONGODB_API_VERSION") {
        Ok(server_api_version) if !server_api_version.is_empty() => Some(ServerApi {
            version: ServerApiVersion::from_str(server_api_version.as_str()).unwrap(),
            deprecation_errors: None,
            strict: None,
        }),
        _ => None,
    });
pub(crate) static SERVERLESS: Lazy<bool> =
    Lazy::new(|| matches!(std::env::var("SERVERLESS"), Ok(s) if s == "serverless"));
pub(crate) static LOAD_BALANCED_SINGLE_URI: Lazy<Option<String>> =
    Lazy::new(|| std::env::var("SINGLE_MONGOS_LB_URI").ok());
pub(crate) static LOAD_BALANCED_MULTIPLE_URI: Lazy<Option<String>> =
    Lazy::new(|| std::env::var("MULTI_MONGOS_LB_URI").ok());
pub(crate) static SERVERLESS_ATLAS_USER: Lazy<Option<String>> =
    Lazy::new(|| std::env::var("SERVERLESS_ATLAS_USER").ok());
pub(crate) static SERVERLESS_ATLAS_PASSWORD: Lazy<Option<String>> =
    Lazy::new(|| std::env::var("SERVERLESS_ATLAS_PASSWORD").ok());

// conditional definitions do not work within the lazy_static! macro, so this
// needs to be defined separately.
#[cfg(feature = "tracing-unstable")]
/// A global default tracing handler that will be installed the first time this
/// value is accessed. A global handler must be used anytime the multi-threaded
/// test runtime is in use, as non-global handlers only apply to the thread
/// they are registered in.
/// By default this handler will collect no tracing events.
/// Its minimum severity levels can be configured on a per-component basis using
/// [`TracingHandler:set_levels`]. The test lock MUST be acquired exclusively in
/// any test that will use the handler to avoid mixing events from multiple tests.
pub(crate) static DEFAULT_GLOBAL_TRACING_HANDLER: Lazy<TracingHandler> = Lazy::new(|| {
    let handler = TracingHandler::new();
    tracing::subscriber::set_global_default(handler.clone())
        .expect("setting global default tracing subscriber failed");
    handler
});

pub(crate) fn update_options_for_testing(options: &mut ClientOptions) {
    if options.server_api.is_none() {
        options.server_api.clone_from(&SERVER_API);
    }

    #[cfg(any(
        feature = "zstd-compression",
        feature = "zlib-compression",
        feature = "snappy-compression"
    ))]
    set_compressor(options);

    if options.credential.is_none() && SERVERLESS_ATLAS_USER.is_some() {
        options.credential = Some(
            Credential::builder()
                .username(SERVERLESS_ATLAS_USER.clone())
                .password(SERVERLESS_ATLAS_PASSWORD.clone())
                .build(),
        );
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

#[cfg(any(
    feature = "zstd-compression",
    feature = "zlib-compression",
    feature = "snappy-compression"
))]
fn set_compressor(options: &mut ClientOptions) {
    use crate::options::Compressor;

    #[cfg(feature = "zstd-compression")]
    {
        options.compressors = Some(vec![Compressor::Zstd { level: None }]);
    }
    #[cfg(feature = "zlib-compression")]
    {
        options.compressors = Some(vec![Compressor::Zlib { level: None }]);
    }
    #[cfg(feature = "snappy-compression")]
    {
        options.compressors = Some(vec![Compressor::Snappy]);
    }
}
