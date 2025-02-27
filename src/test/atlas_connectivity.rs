use crate::{
    bson::{doc, Document},
    client::options::ResolverConfig,
    options::ClientOptions,
    Client,
};

async fn run_test(uri_env_var: &str, resolver_config: Option<ResolverConfig>) {
    let uri = std::env::var(uri_env_var).expect(uri_env_var);

    let options = match resolver_config {
        Some(resolver_config) => {
            ClientOptions::parse(uri)
                .resolver_config(resolver_config)
                .await
        }
        None => ClientOptions::parse(uri).await,
    }
    .expect("uri parsing should succeed");
    let client = Client::with_options(options).expect("option validation should succeed");

    let db = client.database("test");
    db.run_command(doc! { "hello": 1 })
        .await
        .expect("hello should succeed");

    let coll = db.collection::<Document>("test");
    coll.find_one(doc! {})
        .await
        .expect("findOne should succeed");
}

#[tokio::test]
async fn atlas_free_tier_repl_set() {
    run_test("ATLAS_FREE", None).await;
}

#[tokio::test]
async fn atlas_free_tier_repl_set_srv() {
    run_test("ATLAS_SRV_FREE", None).await;
    run_test("ATLAS_SRV_FREE", Some(ResolverConfig::cloudflare())).await;
}

#[tokio::test]
async fn atlas_serverless() {
    run_test("ATLAS_SERVERLESS", None).await;
}

#[tokio::test]
async fn atlas_serverless_srv() {
    run_test("ATLAS_SRV_SERVERLESS", None).await;
    run_test("ATLAS_SRV_SERVERLESS", Some(ResolverConfig::cloudflare())).await;
}

#[tokio::test]
async fn atlas_repl_set() {
    run_test("ATLAS_REPL", None).await;
}

#[tokio::test]
async fn atlas_repl_set_srv() {
    run_test("ATLAS_SRV_REPL", None).await;
    run_test("ATLAS_SRV_REPL", Some(ResolverConfig::cloudflare())).await;
}

#[tokio::test]
async fn atlas_sharded() {
    run_test("ATLAS_SHRD", None).await;
}

#[tokio::test]
async fn atlas_sharded_srv() {
    run_test("ATLAS_SRV_SHRD", None).await;
    run_test("ATLAS_SRV_SHRD", Some(ResolverConfig::cloudflare())).await;
}

#[tokio::test]
async fn atlas_tls_11() {
    run_test("ATLAS_TLS11", None).await;
}

#[tokio::test]
async fn atlas_tls11_srv() {
    run_test("ATLAS_SRV_TLS11", None).await;
    run_test("ATLAS_SRV_TLS11", Some(ResolverConfig::cloudflare())).await;
}

#[tokio::test]
async fn atlas_tls_12() {
    run_test("ATLAS_TLS12", None).await;
}

#[tokio::test]
async fn atlas_tls12_srv() {
    run_test("ATLAS_SRV_TLS12", None).await;
    run_test("ATLAS_SRV_TLS12", Some(ResolverConfig::cloudflare())).await;
}
