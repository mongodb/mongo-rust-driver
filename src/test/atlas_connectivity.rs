use bson::doc;

use crate::Client;

async fn run_test(uri_env_var: &str) {
    if std::env::var_os("MONGO_ATLAS_TESTS").is_none() {
        return;
    }

    let uri = if let Some(uri) = std::env::var_os(uri_env_var) {
        uri
    } else {
        panic!("could not find variable {}", uri_env_var);
    };

    let client = Client::with_uri_str(uri.to_string_lossy().as_ref())
        .await
        .unwrap();

    let db = client.database("test");
    db.run_command(doc! { "isMaster": 1 }, None)
        .await
        .expect("isMaster should succeed");

    let coll = db.collection("test");
    coll.find_one(None, None)
        .await
        .expect("findOne should succeed");
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(core_threads = 2))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn atlas_repl_set() {
    run_test("MONGO_ATLAS_FREE_TIER_REPL_URI").await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(core_threads = 2))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn atlas_repl_set_srv() {
    run_test("MONGO_ATLAS_FREE_TIER_REPL_URI_SRV").await;
}
