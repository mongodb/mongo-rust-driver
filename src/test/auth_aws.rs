use std::env::{remove_var, set_var, var};

use bson::doc;

use crate::{bson::Document, client::auth::aws::test_utils::*, test::DEFAULT_URI, Client};

use super::TestClient;

#[tokio::test]
async fn auth_aws() {
    let client = TestClient::new().await;
    let coll = client.database("aws").collection::<Document>("somecoll");

    coll.find_one(doc! {}).await.unwrap();
}

// The TestClient performs operations upon creation that trigger authentication, so the credential
// caching tests use a regular client instead to avoid that noise.
async fn get_client() -> Client {
    Client::with_uri_str(DEFAULT_URI.clone()).await.unwrap()
}

#[tokio::test]
async fn credential_caching() {
    // This test should only be run when authenticating using AWS endpoints.
    if var("SKIP_CREDENTIAL_CACHING_TESTS").is_ok() {
        return;
    }

    clear_cached_credential().await;

    let client = get_client().await;
    let coll = client.database("aws").collection::<Document>("somecoll");
    coll.find_one(doc! {}).await.unwrap();
    assert!(cached_credential().await.is_some());

    let now = bson::DateTime::now();
    set_cached_expiration(now).await;

    let client = get_client().await;
    let coll = client.database("aws").collection::<Document>("somecoll");
    coll.find_one(doc! {}).await.unwrap();
    assert!(cached_credential().await.is_some());
    assert!(cached_expiration().await > now);

    poison_cached_credential().await;

    let client = get_client().await;
    let coll = client.database("aws").collection::<Document>("somecoll");
    match coll.find_one(doc! {}).await {
        Ok(_) => panic!(
            "find one should have failed with authentication error due to poisoned cached \
             credential"
        ),
        Err(error) => assert!(error.is_auth_error()),
    }
    assert!(cached_credential().await.is_none());

    coll.find_one(doc! {}).await.unwrap();
    assert!(cached_credential().await.is_some());
}

#[tokio::test]
async fn credential_caching_environment_vars() {
    // This test should only be run when authenticating using AWS endpoints.
    if var("SKIP_CREDENTIAL_CACHING_TESTS").is_ok() {
        return;
    }

    clear_cached_credential().await;

    let client = get_client().await;
    let coll = client.database("aws").collection::<Document>("somecoll");
    coll.find_one(doc! {}).await.unwrap();
    assert!(cached_credential().await.is_some());

    set_var("AWS_ACCESS_KEY_ID", cached_access_key_id().await);
    set_var("AWS_SECRET_ACCESS_KEY", cached_secret_access_key().await);
    if let Some(session_token) = cached_session_token().await {
        set_var("AWS_SESSION_TOKEN", session_token);
    }
    clear_cached_credential().await;

    let client = get_client().await;
    let coll = client.database("aws").collection::<Document>("somecoll");
    coll.find_one(doc! {}).await.unwrap();
    assert!(cached_credential().await.is_none());

    set_var("AWS_ACCESS_KEY_ID", "bad");
    set_var("AWS_SECRET_ACCESS_KEY", "bad");
    set_var("AWS_SESSION_TOKEN", "bad");

    let client = get_client().await;
    let coll = client.database("aws").collection::<Document>("somecoll");
    match coll.find_one(doc! {}).await {
        Ok(_) => panic!(
            "find one should have failed with authentication error due to poisoned environment \
             variables"
        ),
        Err(error) => assert!(error.is_auth_error()),
    }

    remove_var("AWS_ACCESS_KEY_ID");
    remove_var("AWS_SECRET_ACCESS_KEY");
    remove_var("AWS_SESSION_TOKEN");
    clear_cached_credential().await;

    let client = get_client().await;
    let coll = client.database("aws").collection::<Document>("somecoll");
    coll.find_one(doc! {}).await.unwrap();
    assert!(cached_credential().await.is_some());

    set_var("AWS_ACCESS_KEY_ID", "bad");
    set_var("AWS_SECRET_ACCESS_KEY", "bad");
    set_var("AWS_SESSION_TOKEN", "bad");

    let client = get_client().await;
    let coll = client.database("aws").collection::<Document>("somecoll");
    coll.find_one(doc! {}).await.unwrap();

    remove_var("AWS_ACCESS_KEY_ID");
    remove_var("AWS_SECRET_ACCESS_KEY");
    remove_var("AWS_SESSION_TOKEN");
}
