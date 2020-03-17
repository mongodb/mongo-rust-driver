use bson::doc;

use crate::Client;

fn run_test(uri_env_var: &str) {
    if std::env::var_os("MONGO_ATLAS_TESTS").is_none() {
        return;
    }

    let uri = if let Some(uri) = std::env::var_os(uri_env_var) {
        uri
    } else {
        panic!("could not find variable {}", uri_env_var);
    };

    let client = Client::with_uri_str(uri.to_string_lossy().as_ref()).unwrap();

    let db = client.database("test");
    db.run_command(doc! { "isMaster": 1 }, None)
        .expect("isMaster should succeed");

    let coll = db.collection("test");
    coll.find_one(None, None).expect("findOne should succeed");
}

#[test]
fn atlas_repl_set() {
    run_test("MONGO_ATLAS_FREE_TIER_REPL_URI");
}

#[test]
fn atlas_repl_set_srv() {
    run_test("MONGO_ATLAS_FREE_TIER_REPL_URI_SRV");
}
