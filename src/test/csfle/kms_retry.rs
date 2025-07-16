//! Prose Test 24. KMS Retry Tests

use std::path::PathBuf;

use mongocrypt::ctx::Algorithm;
use reqwest::{Certificate, Client as HttpClient};

use crate::{
    client_encryption::{AwsMasterKey, AzureMasterKey, ClientEncryption, GcpMasterKey},
    test::get_client_options,
    Client,
    Namespace,
};

use super::{AWS_KMS, AZURE_KMS, CSFLE_TLS_CERT_DIR, GCP_KMS};

#[tokio::test]
async fn kms_retry() {
    let endpoint = "127.0.0.1:9003";

    let mut certificate_file_path = PathBuf::from(&*CSFLE_TLS_CERT_DIR);
    certificate_file_path.push("ca.pem");
    let certificate_file = std::fs::read(&certificate_file_path).unwrap();

    let set_failpoint = |kind: &str, count: u8| {
        // create a fresh client for each request to avoid hangs
        let http_client = HttpClient::builder()
            .add_root_certificate(Certificate::from_pem(&certificate_file).unwrap())
            .build()
            .unwrap();
        let url = format!("https://localhost:9003/set_failpoint/{}", kind);
        let body = format!("{{\"count\":{}}}", count);
        http_client.post(url).body(body).send()
    };

    let aws_kms = AWS_KMS.clone();
    let mut azure_kms = AZURE_KMS.clone();
    azure_kms.1.insert("identityPlatformEndpoint", endpoint);
    let mut gcp_kms = GCP_KMS.clone();
    gcp_kms.1.insert("endpoint", endpoint);
    let mut kms_providers = vec![aws_kms, azure_kms, gcp_kms];

    let tls_options = get_client_options().await.tls_options();
    for kms_provider in kms_providers.iter_mut() {
        kms_provider.2 = tls_options.clone();
    }

    let key_vault_client = Client::for_test().await.into_client();
    let client_encryption = ClientEncryption::new(
        key_vault_client,
        Namespace::new("keyvault", "datakeys"),
        kms_providers,
    )
    .unwrap();

    let aws_master_key = AwsMasterKey::builder()
        .region("foo")
        .key("bar")
        .endpoint(endpoint.to_string())
        .build();
    let azure_master_key = AzureMasterKey::builder()
        .key_vault_endpoint(endpoint)
        .key_name("foo")
        .build();
    let gcp_master_key = GcpMasterKey::builder()
        .project_id("foo")
        .location("bar")
        .key_ring("baz")
        .key_name("qux")
        .endpoint(endpoint.to_string())
        .build();

    // Case 1: createDataKey and encrypt with TCP retry

    // AWS
    set_failpoint("network", 1).await.unwrap();
    let key_id = client_encryption
        .create_data_key(aws_master_key.clone())
        .await
        .unwrap();
    set_failpoint("network", 1).await.unwrap();
    client_encryption
        .encrypt(123, key_id, Algorithm::Deterministic)
        .await
        .unwrap();

    // Azure
    set_failpoint("network", 1).await.unwrap();
    let key_id = client_encryption
        .create_data_key(azure_master_key.clone())
        .await
        .unwrap();
    set_failpoint("network", 1).await.unwrap();
    client_encryption
        .encrypt(123, key_id, Algorithm::Deterministic)
        .await
        .unwrap();

    // GCP
    set_failpoint("network", 1).await.unwrap();
    let key_id = client_encryption
        .create_data_key(gcp_master_key.clone())
        .await
        .unwrap();
    set_failpoint("network", 1).await.unwrap();
    client_encryption
        .encrypt(123, key_id, Algorithm::Deterministic)
        .await
        .unwrap();

    // Case 2: createDataKey and encrypt with HTTP retry

    // AWS
    set_failpoint("http", 1).await.unwrap();
    let key_id = client_encryption
        .create_data_key(aws_master_key.clone())
        .await
        .unwrap();
    set_failpoint("http", 1).await.unwrap();
    client_encryption
        .encrypt(123, key_id, Algorithm::Deterministic)
        .await
        .unwrap();

    // Azure
    set_failpoint("http", 1).await.unwrap();
    let key_id = client_encryption
        .create_data_key(azure_master_key.clone())
        .await
        .unwrap();
    set_failpoint("http", 1).await.unwrap();
    client_encryption
        .encrypt(123, key_id, Algorithm::Deterministic)
        .await
        .unwrap();

    // GCP
    set_failpoint("http", 1).await.unwrap();
    let key_id = client_encryption
        .create_data_key(gcp_master_key.clone())
        .await
        .unwrap();
    set_failpoint("http", 1).await.unwrap();
    client_encryption
        .encrypt(123, key_id, Algorithm::Deterministic)
        .await
        .unwrap();

    // Case 3: createDataKey fails after too many retries

    // AWS
    set_failpoint("network", 4).await.unwrap();
    client_encryption
        .create_data_key(aws_master_key)
        .await
        .unwrap_err();

    // Azure
    set_failpoint("network", 4).await.unwrap();
    client_encryption
        .create_data_key(azure_master_key)
        .await
        .unwrap_err();

    // GCP
    set_failpoint("network", 4).await.unwrap();
    client_encryption
        .create_data_key(gcp_master_key)
        .await
        .unwrap_err();
}
