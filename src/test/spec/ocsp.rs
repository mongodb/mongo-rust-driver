use std::time::Duration;

use bson::doc;

use crate::{
    test::{get_client_options, log_uncaptured},
    Client,
};

#[tokio::test]
async fn run() {
    if std::env::var_os("MONGO_OCSP_TESTS").is_none() {
        log_uncaptured("skipping test due to missing environment variable MONGO_OCSP_TESTS");
        return;
    }

    let should_succeed = std::env::var("OCSP_TLS_SHOULD_SUCCEED")
        .unwrap()
        .to_lowercase();

    let mut options = get_client_options().await.clone();
    let mut tls_options = options.tls_options().unwrap();
    options.server_selection_timeout = Duration::from_millis(200).into();

    let client = Client::with_options(options.clone()).unwrap();
    let response = client
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await;

    match response {
        Ok(_) if should_succeed == "false" => {
            panic!("OSCP: connection succeeded but should have failed")
        }
        Err(e) if should_succeed == "true" => {
            panic!("OSCP: connection failed but should have succeded: {}", e)
        }
        _ => {}
    }

    tls_options.allow_invalid_certificates = Some(true);
    options.tls = Some(tls_options.into());
    let tls_insecure_client = Client::with_options(options).unwrap();
    tls_insecure_client
        .database("admin")
        .run_command(doc! { "ping" : 1 })
        .await
        .expect("tls insecure ping should succeed");
}
