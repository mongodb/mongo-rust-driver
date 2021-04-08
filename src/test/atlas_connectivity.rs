use crate::{bson::doc, options::ClientOptions, Client};
use bson::Document;
use trust_dns_resolver::config::ResolverConfig;

async fn run_test(uri_env_var: &str, resolver_config: Option<ResolverConfig>) {
    if std::env::var_os("MONGO_ATLAS_TESTS").is_none() {
        return;
    }

    let uri = if let Some(uri) = std::env::var_os(uri_env_var) {
        uri
    } else {
        panic!("could not find variable {}", uri_env_var);
    };

    let uri_string = uri.to_string_lossy();
    let options = match resolver_config {
        Some(resolver_config) => {
            ClientOptions::parse_with_resolver_config(uri_string.as_ref(), resolver_config).await
        }
        None => ClientOptions::parse(uri_string.as_ref()).await,
    }
    .expect("uri parsing should succeed");
    let client = Client::with_options(options).expect("option validation should succeed");

    let db = client.database("test");
    db.run_command(doc! { "isMaster": 1 }, None)
        .await
        .expect("isMaster should succeed");

    let coll = db.collection::<Document>("test");
    coll.find_one(None, None)
        .await
        .expect("findOne should succeed");
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn atlas_repl_set() {
    run_test("MONGO_ATLAS_FREE_TIER_REPL_URI", None).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn atlas_repl_set_srv() {
    run_test("MONGO_ATLAS_FREE_TIER_REPL_URI_SRV", None).await;
    run_test(
        "MONGO_ATLAS_FREE_TIER_REPL_URI_SRV",
        Some(ResolverConfig::cloudflare()),
    )
    .await;
}
