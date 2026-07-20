use std::path::PathBuf;

use tokio::sync::OnceCell;

#[cfg(feature = "snappy-compression")]
use crate::options::Compressor;
use crate::{
    bson::{doc, oid::ObjectId},
    bson_util::get_int,
    error::Error,
    options::{
        AuthMechanism,
        ClientOptions,
        Credential,
        ServerApi,
        ServerApiVersion,
        Tls,
        TlsOptions,
    },
    Client,
};

async fn run_test(options: ClientOptions) {
    let is_authenticated = options.credential.is_some();
    let client = Client::with_options(options).expect("failed to create client");

    // ping
    let response = client
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await
        .unwrap();
    let ok = response
        .get("ok")
        .and_then(get_int)
        .expect("ping missing ok");
    assert_eq!(ok, 1);

    // connection status
    let response = client
        .database("admin")
        .run_command(doc! { "connectionStatus": 1 })
        .await
        .unwrap();
    let ok = response
        .get("ok")
        .and_then(get_int)
        .expect("connectionStatus missing ok");
    assert_eq!(ok, 1);
    let authenticated_users = response
        .get_document("authInfo")
        .and_then(|info| info.get_array("authenticatedUsers"))
        .expect("connectionStatus missing authenticatedUsers");
    assert_eq!(is_authenticated, !authenticated_users.is_empty());

    // crud
    if is_authenticated {
        let collection = client
            .database("db")
            .collection(&format!("sfp_test_{}", ObjectId::new()));
        let crud_result = async {
            let inserted_id = collection.insert_one(doc! { "x": 1 }).await?.inserted_id;
            collection
                .find_one(doc! { "_id": inserted_id })
                .await?
                .ok_or_else(|| Error::custom("find did not return document"))
        }
        .await;
        collection.drop().await.unwrap();
        crud_result.unwrap();
    }
}

fn get_var(var: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| panic!("{var} must be defined"))
}

async fn get_standard_options() -> ClientOptions {
    static OPTIONS: OnceCell<ClientOptions> = OnceCell::const_new();
    OPTIONS
        .get_or_init(|| async {
            let uri = get_var("SFP_ATLAS_URI");
            ClientOptions::parse(uri)
                .await
                .expect("failed to parse SFP URI")
        })
        .await
        .clone()
}

async fn get_scram_options() -> ClientOptions {
    static OPTIONS: OnceCell<ClientOptions> = OnceCell::const_new();
    OPTIONS
        .get_or_init(|| async {
            let mut options = get_standard_options().await;
            options.credential = Some(
                Credential::builder()
                    .username(get_var("SFP_ATLAS_USER"))
                    .password(get_var("SFP_ATLAS_PASSWORD"))
                    .build(),
            );
            options
        })
        .await
        .clone()
}

async fn get_x509_options() -> ClientOptions {
    static OPTIONS: OnceCell<ClientOptions> = OnceCell::const_new();
    OPTIONS
        .get_or_init(|| async {
            let uri = get_var("SFP_ATLAS_X509_URI");
            let cert_path = PathBuf::from(get_var("SFP_ATLAS_X509_CERT"));
            let mut options = ClientOptions::parse(uri)
                .await
                .expect("failed to parse SFP X509 URI");
            options.credential = Some(
                Credential::builder()
                    .mechanism(AuthMechanism::MongoDbX509)
                    .build(),
            );
            match options.tls {
                Some(Tls::Enabled(ref mut options)) => options.cert_key_file_path = Some(cert_path),
                _ => {
                    options.tls = Some(Tls::Enabled(
                        TlsOptions::builder().cert_key_file_path(cert_path).build(),
                    ));
                }
            }
            options
        })
        .await
        .clone()
}

#[tokio::test]
async fn unauthenticated() {
    run_test(get_standard_options().await).await;
}

#[tokio::test]
async fn scram() {
    run_test(get_scram_options().await).await;
}

#[tokio::test]
async fn scram_compression() {
    #[cfg(not(feature = "snappy-compression"))]
    {
        panic!("snappy-compression must be enabled");
    }
    #[cfg(feature = "snappy-compression")]
    {
        let mut options = get_scram_options().await;
        options.compressors = Some(vec![Compressor::Snappy]);
        run_test(options).await;
    }
}

#[tokio::test]
async fn scram_stable_api() {
    let mut options = get_scram_options().await;
    options.server_api = Some(ServerApi::builder().version(ServerApiVersion::V1).build());
    run_test(options).await;
}

#[tokio::test]
async fn x509() {
    run_test(get_x509_options().await).await;
}

#[tokio::test]
async fn x509_compression() {
    #[cfg(not(feature = "snappy-compression"))]
    {
        panic!("snappy-compression must be enabled");
    }
    #[cfg(feature = "snappy-compression")]
    {
        let mut options = get_x509_options().await;
        options.compressors = Some(vec![Compressor::Snappy]);
        run_test(options).await;
    }
}

#[tokio::test]
async fn x509_stable_api() {
    let mut options = get_x509_options().await;
    options.server_api = Some(ServerApi::builder().version(ServerApiVersion::V1).build());
    run_test(options).await;
}
