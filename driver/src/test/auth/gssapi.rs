use std::sync::LazyLock;

use percent_encoding::{percent_encode, NON_ALPHANUMERIC};

use crate::{
    bson::{doc, Document},
    test::get_var,
    Client,
};

static SASL_HOST: LazyLock<String> = LazyLock::new(|| get_var("SASL_HOST"));
static SASL_PORT: LazyLock<String> = LazyLock::new(|| get_var("SASL_PORT"));
#[cfg(target_os = "windows")]
static SASL_PASS: LazyLock<String> = LazyLock::new(|| get_var("SASL_PASS"));
static PRINCIPAL: LazyLock<String> = LazyLock::new(|| get_var("PRINCIPAL"));
static GSSAPI_DB: LazyLock<String> = LazyLock::new(|| get_var("GSSAPI_DB"));

async fn run_test(canonicalize_host_name: Option<&str>) {
    let user = percent_encode(PRINCIPAL.as_bytes(), NON_ALPHANUMERIC).collect::<String>();
    #[cfg(target_os = "windows")]
    let password = format!(
        ":{}",
        percent_encode(SASL_PASS.as_bytes(), NON_ALPHANUMERIC).collect::<String>()
    );
    #[cfg(not(target_os = "windows"))]
    let password = "";
    let mut uri = format!(
        "mongodb://{user}{password}@{}:{}/{}?authMechanism=GSSAPI",
        *SASL_HOST, *SASL_PORT, *GSSAPI_DB
    );
    if let Some(canonicalize_host_name) = canonicalize_host_name {
        uri.push_str("&authMechanismProperties=CANONICALIZE_HOST_NAME:");
        uri.push_str(canonicalize_host_name);
    }
    let client = Client::with_uri_str(uri).await.unwrap();

    client
        .database(&*GSSAPI_DB)
        .collection::<Document>("test")
        .find_one(doc! {})
        .await
        .unwrap();
}

#[tokio::test]
async fn no_auth_mechanism_properties() {
    run_test(None).await;
}

#[tokio::test]
async fn canonicalize_host_name_false() {
    run_test(Some("false")).await;
}

#[tokio::test]
async fn canonicalize_host_name_forward() {
    run_test(Some("forward")).await;
}

#[tokio::test]
async fn canonicalize_host_name_forward_and_reverse() {
    run_test(Some("forwardAndReverse")).await;
}

#[tokio::test]
async fn bad_user() {
    let uri = format!(
        "mongodb://baduser@{}:{}/{}?authMechanism=GSSAPI",
        *SASL_HOST, *SASL_PORT, *GSSAPI_DB
    );
    let client = Client::with_uri_str(uri).await.unwrap();

    let error = client
        .database(&*GSSAPI_DB)
        .collection::<Document>("test")
        .find_one(doc! {})
        .await
        .unwrap_err();
    assert!(error.is_auth_error());
}
