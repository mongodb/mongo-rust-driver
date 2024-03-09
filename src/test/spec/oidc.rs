use crate::{
    client::{
        auth::{oidc, AuthMechanism, Credential},
        options::ClientOptions,
    },
    test::log_uncaptured,
    Client,
};
use std::sync::{Arc, Mutex};

// simple_cb is a simple callback that increments the passed atomic integer and returns a static IdpServerResponse
async fn simple_cb(call_count: Arc<Mutex<u32>>) -> crate::error::Result<oidc::IdpServerResponse> {
    *call_count.lock().unwrap() += 1;
    Ok(oidc::IdpServerResponse {
        access_token: tokio::fs::read_to_string("/tmp/tokens/test_user1").await?,
        expires: None,
        refresh_token: None,
    })
}

// Machine Callback tests
// Prose test 1.1 Single Principal Implicit Username
#[tokio::test]
async fn machine_single_principal_implicit_username() -> anyhow::Result<()> {
    use bson::Document;
    use futures_util::FutureExt;

    if std::env::var("OIDC_TOKEN_DIR").is_err() {
        log_uncaptured("Skipping OIDC test");
        return Ok(());
    }

    // we need to assert that the callback is only called once
    let call_count = Arc::new(Mutex::new(0));
    let cb_call_count = call_count.clone();

    let mut opts = ClientOptions::parse("mongodb://localhost/?authMechanism=MONGODB-OIDC").await?;
    opts.credential = Credential::builder()
        .mechanism(AuthMechanism::MongoDbOidc)
        .oidc_callback(oidc::Callback::machine(move |_| {
            { simple_cb(cb_call_count.clone()) }.boxed()
        }))
        .build()
        .into();
    let client = Client::with_options(opts)?;
    client
        .database("test")
        .collection::<Document>("test")
        .find_one(None, None)
        .await?;
    assert_eq!(1, *(*call_count).lock().unwrap());
    Ok(())
}

// Human Callback tests
// Prose test 1.1 Single Principal Implicit Username
#[tokio::test]
async fn human_single_principal_implicit_username() -> anyhow::Result<()> {
    use crate::{
        client::{
            auth::{oidc, AuthMechanism, Credential},
            options::ClientOptions,
        },
        test::log_uncaptured,
        Client,
    };
    use bson::Document;
    use futures_util::FutureExt;

    if std::env::var("OIDC_TOKEN_DIR").is_err() {
        log_uncaptured("Skipping OIDC test");
        return Ok(());
    }

    // we need to assert that the callback is only called once
    let call_count = Arc::new(Mutex::new(0));
    let cb_call_count = call_count.clone();

    let mut opts = ClientOptions::parse("mongodb://localhost/?authMechanism=MONGODB-OIDC").await?;
    opts.credential = Credential::builder()
        .mechanism(AuthMechanism::MongoDbOidc)
        .oidc_callback(oidc::Callback::human(move |_| {
            { simple_cb(cb_call_count.clone()) }.boxed()
        }))
        .build()
        .into();
    let client = Client::with_options(opts)?;
    client
        .database("test")
        .collection::<Document>("test")
        .find_one(None, None)
        .await?;
    assert_eq!(1, *(*call_count).lock().unwrap());
    Ok(())
}
