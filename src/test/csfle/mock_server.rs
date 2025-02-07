use std::{path::PathBuf, time::Duration};

use bson::{doc, spec::BinarySubtype, Bson, Document, RawBson};
use futures_util::TryStreamExt;
use mongocrypt::ctx::{Algorithm, KmsProvider, KmsProviderType};

use crate::{
    action::Action,
    client_encryption::{
        AwsMasterKey,
        AzureMasterKey,
        ClientEncryption,
        EncryptKey,
        GcpMasterKey,
        KmipMasterKey,
        LocalMasterKey,
        MasterKey,
    },
    error::ErrorKind,
    options::{Credential, TlsOptions},
    test::get_client_options,
    Client,
};

use super::*;

// Prose test 2. Data Key and Double Encryption
#[tokio::test]
async fn data_key_double_encryption() -> Result<()> {
    // Setup: drop stale data.
    let (client, _) = init_client().await?;

    // Setup: client with auto encryption.
    let schema_map = [(
        "db.coll",
        doc! {
            "bsonType": "object",
            "properties": {
                "encrypted_placeholder": {
                    "encrypt": {
                        "keyId": "/placeholder",
                        "bsonType": "string",
                        "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                    }
                }
            }
        },
    )];
    let client_encrypted = Client::encrypted_builder(
        get_client_options().await.clone(),
        KV_NAMESPACE.clone(),
        UNNAMED_KMS_PROVIDERS.clone(),
    )?
    .schema_map(schema_map)
    .extra_options(EXTRA_OPTIONS.clone())
    .disable_crypt_shared(*DISABLE_CRYPT_SHARED)
    .build()
    .await?;

    // Setup: manual encryption.
    let client_encryption = ClientEncryption::new(
        client.clone().into_client(),
        KV_NAMESPACE.clone(),
        UNNAMED_KMS_PROVIDERS.clone(),
    )?;

    // Testing each provider:

    let mut events = client.events.stream();
    let provider_keys: [(KmsProvider, MasterKey); 5] = [
        (
            KmsProvider::aws(),
            AwsMasterKey::builder()
                .region("us-east-1")
                .key("arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0")
                .build()
                .into(),
        ),
        (
            KmsProvider::azure(),
            AzureMasterKey::builder()
                .key_vault_endpoint("key-vault-csfle.vault.azure.net")
                .key_name("key-name-csfle")
                .build()
                .into(),
        ),
        (
            KmsProvider::gcp(),
            GcpMasterKey::builder()
                .project_id("devprod-drivers")
                .location("global")
                .key_ring("key-ring-csfle")
                .key_name("key-name-csfle")
                .build()
                .into(),
        ),
        (
            KmsProvider::local(),
            LocalMasterKey::builder().build().into(),
        ),
        (KmsProvider::kmip(), KmipMasterKey::builder().build().into()),
    ];
    for (provider, master_key) in provider_keys {
        // Create a data key
        let datakey_id = client_encryption
            .create_data_key(master_key)
            .key_alt_names([format!("{}_altname", provider.as_string())])
            .await?;
        assert_eq!(datakey_id.subtype, BinarySubtype::Uuid);
        let docs: Vec<_> = client
            .database("keyvault")
            .collection::<Document>("datakeys")
            .find(doc! { "_id": datakey_id.clone() })
            .await?
            .try_collect()
            .await?;
        assert_eq!(docs.len(), 1);
        assert_eq!(
            docs[0].get_document("masterKey")?.get_str("provider")?,
            provider.as_string()
        );
        let found = events
            .next_match(
                Duration::from_millis(500),
                ok_pred(|ev| {
                    let ev = match ev.as_command_started_event() {
                        Some(e) => e,
                        None => return Ok(false),
                    };
                    if ev.command_name != "insert" {
                        return Ok(false);
                    }
                    let cmd = &ev.command;
                    if cmd.get_document("writeConcern")?.get_str("w")? != "majority" {
                        return Ok(false);
                    }
                    Ok(cmd.get_array("documents")?.iter().any(|doc| {
                        matches!(
                            doc.as_document().and_then(|d| d.get("_id")),
                            Some(Bson::Binary(id)) if id == &datakey_id
                        )
                    }))
                }),
            )
            .await;
        assert!(found.is_some(), "no valid event found");

        // Manually encrypt a value and automatically decrypt it.
        let encrypted = client_encryption
            .encrypt(
                format!("hello {}", provider.as_string()),
                EncryptKey::Id(datakey_id),
                Algorithm::Deterministic,
            )
            .await?;
        assert_eq!(encrypted.subtype, BinarySubtype::Encrypted);
        let coll = client_encrypted
            .database("db")
            .collection::<Document>("coll");
        coll.insert_one(doc! { "_id": provider.as_string(), "value": encrypted.clone() })
            .await?;
        let found = coll.find_one(doc! { "_id": provider.as_string() }).await?;
        assert_eq!(
            found.as_ref().and_then(|doc| doc.get("value")),
            Some(&Bson::String(format!("hello {}", provider.as_string()))),
        );

        // Manually encrypt a value via key alt name.
        let other_encrypted = client_encryption
            .encrypt(
                format!("hello {}", provider.as_string()),
                EncryptKey::AltName(format!("{}_altname", provider.as_string())),
                Algorithm::Deterministic,
            )
            .await?;
        assert_eq!(other_encrypted.subtype, BinarySubtype::Encrypted);
        assert_eq!(other_encrypted.bytes, encrypted.bytes);

        // Attempt to auto-encrypt an already encrypted field.
        let result = coll
            .insert_one(doc! { "encrypted_placeholder": encrypted })
            .await;
        let err = result.unwrap_err();
        assert!(
            matches!(*err.kind, ErrorKind::Encryption(..)) || err.is_command_error(),
            "unexpected error: {}",
            err
        );
    }

    Ok(())
}

// Prose test 3. External Key Vault Test
#[tokio::test]
async fn external_key_vault() -> Result<()> {
    for with_external_key_vault in [false, true] {
        // Setup: initialize db.
        let (client, datakeys) = init_client().await?;
        datakeys
            .insert_one(load_testdata("external/external-key.json")?)
            .await?;

        // Setup: test options.
        let kv_client = if with_external_key_vault {
            let mut opts = get_client_options().await.clone();
            opts.credential = Some(
                Credential::builder()
                    .username("fake-user".to_string())
                    .password("fake-pwd".to_string())
                    .build(),
            );
            Some(Client::with_options(opts)?)
        } else {
            None
        };

        // Setup: encrypted client.
        let client_encrypted = Client::encrypted_builder(
            get_client_options().await.clone(),
            KV_NAMESPACE.clone(),
            vec![LOCAL_KMS.clone()],
        )?
        .key_vault_client(kv_client.clone())
        .schema_map([("db.coll", load_testdata("external/external-schema.json")?)])
        .extra_options(EXTRA_OPTIONS.clone())
        .disable_crypt_shared(*DISABLE_CRYPT_SHARED)
        .build()
        .await?;
        // Setup: manual encryption.
        let client_encryption = ClientEncryption::new(
            kv_client.unwrap_or_else(|| client.into_client()),
            KV_NAMESPACE.clone(),
            vec![LOCAL_KMS.clone()],
        )?;

        // Test: encrypted client.
        let result = client_encrypted
            .database("db")
            .collection::<Document>("coll")
            .insert_one(doc! { "encrypted": "test" })
            .await;
        if with_external_key_vault {
            let err = result.unwrap_err();
            assert!(err.is_auth_error(), "unexpected error: {}", err);
        } else {
            assert!(
                result.is_ok(),
                "unexpected error: {}",
                result.err().unwrap()
            );
        }
        // Test: manual encryption.
        let result = client_encryption
            .encrypt(
                "test",
                EncryptKey::Id(base64_uuid("LOCALAAAAAAAAAAAAAAAAA==")?),
                Algorithm::Deterministic,
            )
            .await;
        if with_external_key_vault {
            let err = result.unwrap_err();
            assert!(err.is_auth_error(), "unexpected error: {}", err);
        } else {
            assert!(
                result.is_ok(),
                "unexpected error: {}",
                result.err().unwrap()
            );
        }
    }

    Ok(())
}

mod corpus {
    use super::*;

    async fn run_corpus_test(local_schema: bool) -> Result<()> {
        // Setup: db initialization.
        let (client, datakeys) = init_client().await?;
        let schema = load_testdata("corpus/corpus-schema.json")?;
        let validator = if local_schema {
            None
        } else {
            Some(doc! { "$jsonSchema": schema.clone() })
        };
        client
            .database("db")
            .create_collection("coll")
            .optional(validator, |b, v| b.validator(v))
            .await?;
        for f in [
            "corpus/corpus-key-local.json",
            "corpus/corpus-key-aws.json",
            "corpus/corpus-key-azure.json",
            "corpus/corpus-key-gcp.json",
            "corpus/corpus-key-kmip.json",
        ] {
            datakeys.insert_one(load_testdata(f)?).await?;
        }

        // Setup: encrypted client and manual encryption.
        let client_encrypted = {
            let mut enc_builder = Client::encrypted_builder(
                get_client_options().await.clone(),
                KV_NAMESPACE.clone(),
                UNNAMED_KMS_PROVIDERS.clone(),
            )?
            .extra_options(EXTRA_OPTIONS.clone())
            .disable_crypt_shared(*DISABLE_CRYPT_SHARED);
            if local_schema {
                enc_builder = enc_builder.schema_map([("db.coll", schema)]);
            }
            enc_builder.build().await?
        };
        let client_encryption = ClientEncryption::new(
            client.clone().into_client(),
            KV_NAMESPACE.clone(),
            UNNAMED_KMS_PROVIDERS.clone(),
        )?;

        // Test: build corpus.
        let corpus = load_corpus_nodecimal128("corpus/corpus.json")?;
        let mut corpus_copied = doc! {};
        for (name, field) in &corpus {
            // Copy simple fields
            if [
                "_id",
                "altname_aws",
                "altname_local",
                "altname_azure",
                "altname_gcp",
                "altname_kmip",
            ]
            .contains(&name.as_str())
            {
                corpus_copied.insert(name, field);
                continue;
            }
            // Encrypt `value` field in subdocuments.
            let subdoc = match field.as_document() {
                Some(d) => d,
                None => {
                    return Err(failure!(
                        "unexpected field type for {:?}: {:?}",
                        name,
                        field.element_type()
                    ))
                }
            };
            let method = subdoc.get_str("method")?;
            if method == "auto" {
                corpus_copied.insert(name, subdoc);
                continue;
            }
            if method != "explicit" {
                return Err(failure!("Invalid method {:?}", method));
            }
            let algo = match subdoc.get_str("algo")? {
                "rand" => Algorithm::Random,
                "det" => Algorithm::Deterministic,
                s => return Err(failure!("Invalid algorithm {:?}", s)),
            };
            let kms = KmsProvider::from_string(subdoc.get_str("kms")?);
            let key = match subdoc.get_str("identifier")? {
                "id" => EncryptKey::Id(base64_uuid(match kms.provider_type() {
                    KmsProviderType::Local => "LOCALAAAAAAAAAAAAAAAAA==",
                    KmsProviderType::Aws => "AWSAAAAAAAAAAAAAAAAAAA==",
                    KmsProviderType::Azure => "AZUREAAAAAAAAAAAAAAAAA==",
                    KmsProviderType::Gcp => "GCPAAAAAAAAAAAAAAAAAAA==",
                    KmsProviderType::Kmip => "KMIPAAAAAAAAAAAAAAAAAA==",
                    _ => return Err(failure!("Invalid kms provider {:?}", kms)),
                })?),
                "altname" => EncryptKey::AltName(kms.as_string()),
                s => return Err(failure!("Invalid identifier {:?}", s)),
            };
            let value: RawBson = subdoc
                .get("value")
                .expect("no value to encrypt")
                .clone()
                .try_into()?;
            let result = client_encryption.encrypt(value, key, algo).await;
            let mut subdoc_copied = subdoc.clone();
            if subdoc.get_bool("allowed")? {
                subdoc_copied.insert("value", result?);
            } else {
                result.expect_err("expected encryption to be disallowed");
            }
            corpus_copied.insert(name, subdoc_copied);
        }

        // Test: insert into and find from collection, with automatic encryption.
        let coll = client_encrypted
            .database("db")
            .collection::<Document>("coll");
        let id = coll.insert_one(corpus_copied).await?.inserted_id;
        let corpus_decrypted = coll
            .find_one(doc! { "_id": id.clone() })
            .await?
            .expect("document lookup failed");
        assert_eq!(corpus, corpus_decrypted);

        // Test: validate encrypted form.
        let corpus_encrypted_expected = load_corpus_nodecimal128("corpus/corpus-encrypted.json")?;
        let corpus_encrypted_actual = client
            .database("db")
            .collection::<Document>("coll")
            .find_one(doc! { "_id": id })
            .await?
            .expect("encrypted document lookup failed");
        for (name, field) in &corpus_encrypted_expected {
            let subdoc = match field.as_document() {
                Some(d) => d,
                None => continue,
            };
            let value = subdoc.get("value").expect("no expected value");
            let actual_value = corpus_encrypted_actual
                .get_document(name)?
                .get("value")
                .expect("no actual value");
            let algo = subdoc.get_str("algo")?;
            if algo == "det" {
                assert_eq!(value, actual_value);
            }
            let allowed = subdoc.get_bool("allowed")?;
            if algo == "rand" && allowed {
                assert_ne!(value, actual_value);
            }
            if allowed {
                let bin = match value {
                    bson::Bson::Binary(b) => b,
                    _ => {
                        return Err(failure!(
                            "expected value {:?} should be Binary, got {:?}",
                            name,
                            value
                        ))
                    }
                };
                let actual_bin = match actual_value {
                    bson::Bson::Binary(b) => b,
                    _ => {
                        return Err(failure!(
                            "actual value {:?} should be Binary, got {:?}",
                            name,
                            actual_value
                        ))
                    }
                };
                let dec = client_encryption.decrypt(bin.as_raw_binary()).await?;
                let actual_dec = client_encryption
                    .decrypt(actual_bin.as_raw_binary())
                    .await?;
                assert_eq!(dec, actual_dec);
            } else {
                assert_eq!(Some(value), corpus.get_document(name)?.get("value"));
            }
        }

        Ok(())
    }

    // Prose test 6. Corpus Test (collection schema)
    #[tokio::test]
    async fn corpus_coll_schema() -> Result<()> {
        run_corpus_test(false).await?;
        Ok(())
    }

    // Prose test 6. Corpus Test (local schema)
    #[tokio::test]
    async fn corpus_local_schema() -> Result<()> {
        run_corpus_test(true).await?;
        Ok(())
    }
}

// Prose test 7. Custom Endpoint Test (case 10. kmip, no endpoint)
#[tokio::test]
async fn custom_endpoint_kmip_no_endpoint() -> Result<()> {
    let master_key = KmipMasterKey::builder()
        .key_id(Some("1".to_string()))
        .build();

    let client_encryption = custom_endpoint_setup(true).await?;
    let key_id = client_encryption
        .create_data_key(master_key.clone())
        .await?;
    validate_roundtrip(&client_encryption, key_id).await?;

    let client_encryption_invalid = custom_endpoint_setup(false).await?;
    let result = client_encryption_invalid.create_data_key(master_key).await;
    assert!(result.unwrap_err().is_network_error());

    Ok(())
}

// Prose test 7. Custom Endpoint Test (case 11. kmip, valid endpoint)
#[tokio::test]
async fn custom_endpoint_kmip_valid_endpoint() -> Result<()> {
    let master_key = KmipMasterKey::builder()
        .key_id(Some("1".to_string()))
        .endpoint(Some("localhost:5698".to_string()))
        .build();

    let client_encryption = custom_endpoint_setup(true).await?;
    let key_id = client_encryption.create_data_key(master_key).await?;
    validate_roundtrip(&client_encryption, key_id).await
}

mod kms_tls {
    use super::*;

    const KMS_EXPIRED: &str = "127.0.0.1:9000";
    const KMS_WRONG_HOST: &str = "127.0.0.1:9001";
    const KMS_CORRECT: &str = "127.0.0.1:9002";

    async fn run_kms_tls_test(endpoint: impl Into<String>) -> crate::error::Result<()> {
        // Setup
        let kv_client = Client::for_test().await;
        let client_encryption = ClientEncryption::new(
            kv_client.clone().into_client(),
            KV_NAMESPACE.clone(),
            UNNAMED_KMS_PROVIDERS.clone(),
        )?;

        // Test
        client_encryption
            .create_data_key(
                AwsMasterKey::builder()
                    .region("us-east-1")
                    .key(
                        "arn:aws:kms:us-east-1:579766882180:key/\
                         89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
                    )
                    .endpoint(Some(endpoint.into()))
                    .build(),
            )
            .await
            .map(|_| ())
    }

    // Prose test 10. KMS TLS Tests
    #[tokio::test]
    async fn kms_tls() -> Result<()> {
        // Invalid KMS Certificate
        let err = run_kms_tls_test(KMS_EXPIRED).await.unwrap_err();
        assert!(
            err.to_string().contains("certificate verify failed"),
            "unexpected error: {}",
            err
        );

        // Invalid Hostname in KMS Certificate
        let err = run_kms_tls_test(KMS_WRONG_HOST).await.unwrap_err();
        assert!(
            err.to_string().contains("certificate verify failed"),
            "unexpected error: {}",
            err
        );

        Ok(())
    }

    // Prose test 11. KMS TLS Options Tests
    #[tokio::test]
    async fn kms_tls_options() -> Result<()> {
        fn update_providers(
            mut base_providers: KmsProviderList,
            new_tls_options: TlsOptions,
            mut update_credentials: impl FnMut(&KmsProvider, &mut Document),
        ) -> KmsProviderList {
            for (provider, credentials, tls_options) in base_providers.iter_mut() {
                if provider != &KmsProvider::local() {
                    *tls_options = Some(new_tls_options.clone());
                }
                update_credentials(provider, credentials);
            }
            base_providers
        }

        let cert_dir = PathBuf::from(&*CSFLE_TLS_CERT_DIR);
        let ca_path = cert_dir.join("ca.pem");
        let key_path = cert_dir.join("client.pem");

        let add_correct_credentials =
            |provider: &KmsProvider, credentials: &mut Document| match provider.provider_type() {
                KmsProviderType::Azure => {
                    credentials.insert("identityPlatformEndpoint", KMS_CORRECT);
                }
                KmsProviderType::Gcp => {
                    credentials.insert("endpoint", KMS_CORRECT);
                }
                _ => {}
            };
        let add_expired_credentials =
            |provider: &KmsProvider, credentials: &mut Document| match provider.provider_type() {
                KmsProviderType::Azure => {
                    credentials.insert("identityPlatformEndpoint", KMS_EXPIRED);
                }
                KmsProviderType::Gcp | KmsProviderType::Kmip => {
                    credentials.insert("endpoint", KMS_EXPIRED);
                }
                _ => {}
            };
        let add_wrong_host_credentials =
            |provider: &KmsProvider, credentials: &mut Document| match provider.provider_type() {
                KmsProviderType::Azure => {
                    credentials.insert("identityPlatformEndpoint", KMS_WRONG_HOST);
                }
                KmsProviderType::Gcp | KmsProviderType::Kmip => {
                    credentials.insert("endpoint", KMS_WRONG_HOST);
                }
                _ => {}
            };

        let providers_no_client_cert = update_providers(
            UNNAMED_KMS_PROVIDERS.clone(),
            TlsOptions::builder().ca_file_path(ca_path.clone()).build(),
            add_correct_credentials,
        );
        let client_encryption_no_client_cert = ClientEncryption::new(
            Client::for_test().await.into_client(),
            KV_NAMESPACE.clone(),
            providers_no_client_cert.clone(),
        )?;

        let providers_with_tls = update_providers(
            UNNAMED_KMS_PROVIDERS.clone(),
            TlsOptions::builder()
                .ca_file_path(ca_path.clone())
                .cert_key_file_path(key_path.clone())
                .build(),
            add_correct_credentials,
        );
        let client_encryption_with_tls = ClientEncryption::new(
            Client::for_test().await.into_client(),
            KV_NAMESPACE.clone(),
            providers_with_tls.clone(),
        )?;

        let client_encryption_expired = ClientEncryption::new(
            Client::for_test().await.into_client(),
            KV_NAMESPACE.clone(),
            update_providers(
                UNNAMED_KMS_PROVIDERS.clone(),
                TlsOptions::builder().ca_file_path(ca_path.clone()).build(),
                add_expired_credentials,
            ),
        )?;

        let client_encryption_invalid_hostname = ClientEncryption::new(
            Client::for_test().await.into_client(),
            KV_NAMESPACE.clone(),
            update_providers(
                UNNAMED_KMS_PROVIDERS.clone(),
                TlsOptions::builder().ca_file_path(ca_path.clone()).build(),
                add_wrong_host_credentials,
            ),
        )?;

        let mut named_providers = providers_no_client_cert
            .into_iter()
            .filter_map(|info| {
                if !matches!(info.0.provider_type(), KmsProviderType::Local) {
                    Some(add_name_to_info(info, "no_client_cert"))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        named_providers.extend(providers_with_tls.into_iter().filter_map(|info| {
            if !matches!(info.0.provider_type(), KmsProviderType::Local) {
                Some(add_name_to_info(info, "with_tls"))
            } else {
                None
            }
        }));
        let client_encryption_with_names = ClientEncryption::new(
            Client::for_test().await.into_client(),
            KV_NAMESPACE.clone(),
            named_providers,
        )?;

        async fn provider_test(
            client_encryption: &ClientEncryption,
            master_key: impl Into<MasterKey>,
            expected_errs: &[&str],
        ) -> Result<()> {
            let err = client_encryption
                .create_data_key(master_key)
                .await
                .unwrap_err();
            let err_str = err.to_string();
            if !expected_errs.iter().any(|s| err_str.contains(s)) {
                Err(err)?
            }
            Ok(())
        }

        // Case 1: AWS
        fn aws_key(endpoint: impl Into<String>) -> AwsMasterKey {
            AwsMasterKey::builder()
                .region("us-east-1")
                .key("arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0")
                .endpoint(Some(endpoint.into()))
                .build()
        }

        provider_test(
            &client_encryption_no_client_cert,
            aws_key(KMS_CORRECT),
            &["SSL routines", "connection was forcibly closed"],
        )
        .await?;
        provider_test(
            &client_encryption_with_tls,
            aws_key(KMS_CORRECT),
            &["parse error"],
        )
        .await?;
        provider_test(
            &client_encryption_expired,
            aws_key(KMS_EXPIRED),
            &["certificate verify failed"],
        )
        .await?;
        provider_test(
            &client_encryption_invalid_hostname,
            aws_key(KMS_WRONG_HOST),
            &["certificate verify failed"],
        )
        .await?;

        // Case 2: Azure
        let azure_key = AzureMasterKey::builder()
            .key_vault_endpoint("doesnotexist.local")
            .key_name("foo")
            .build();

        provider_test(
            &client_encryption_no_client_cert,
            azure_key.clone(),
            &["SSL routines", "connection was forcibly closed"],
        )
        .await?;
        provider_test(
            &client_encryption_with_tls,
            azure_key.clone(),
            &["HTTP status=404"],
        )
        .await?;
        provider_test(
            &client_encryption_expired,
            azure_key.clone(),
            &["certificate verify failed"],
        )
        .await?;
        provider_test(
            &client_encryption_invalid_hostname,
            azure_key.clone(),
            &["certificate verify failed"],
        )
        .await?;

        // Case 3: GCP
        let gcp_key = GcpMasterKey::builder()
            .project_id("foo")
            .location("bar")
            .key_ring("baz")
            .key_name("foo")
            .build();

        provider_test(
            &client_encryption_no_client_cert,
            gcp_key.clone(),
            &["SSL routines", "connection was forcibly closed"],
        )
        .await?;
        provider_test(
            &client_encryption_with_tls,
            gcp_key.clone(),
            &["HTTP status=404"],
        )
        .await?;
        provider_test(
            &client_encryption_expired,
            gcp_key.clone(),
            &["certificate verify failed"],
        )
        .await?;
        provider_test(
            &client_encryption_invalid_hostname,
            gcp_key.clone(),
            &["certificate verify failed"],
        )
        .await?;

        // Case 4: KMIP
        let kmip_key = KmipMasterKey::builder().build();

        provider_test(
            &client_encryption_no_client_cert,
            kmip_key.clone(),
            &["SSL routines", "connection was forcibly closed"],
        )
        .await?;
        // This one succeeds!
        client_encryption_with_tls
            .create_data_key(kmip_key.clone())
            .await?;
        provider_test(
            &client_encryption_expired,
            kmip_key.clone(),
            &["certificate verify failed"],
        )
        .await?;
        provider_test(
            &client_encryption_invalid_hostname,
            kmip_key.clone(),
            &["certificate verify failed"],
        )
        .await?;

        // Case 6: named KMS providers apply TLS options
        // Named AWS
        let mut master_key = aws_key("127.0.0.1:9002");
        master_key.name = Some("no_client_cert".to_string());
        provider_test(
            &client_encryption_with_names,
            master_key,
            &["SSL routines", "connection was forcibly closed"],
        )
        .await?;

        let mut master_key = aws_key("127.0.0.1:9002");
        master_key.name = Some("with_tls".to_string());
        provider_test(&client_encryption_with_names, master_key, &["parse error"]).await?;

        // Named Azure
        let mut master_key = azure_key.clone();
        master_key.name = Some("no_client_cert".to_string());
        provider_test(
            &client_encryption_with_names,
            master_key,
            &["SSL routines", "connection was forcibly closed"],
        )
        .await?;

        let mut master_key = azure_key.clone();
        master_key.name = Some("with_tls".to_string());
        provider_test(
            &client_encryption_with_names,
            master_key,
            &["HTTP status=404"],
        )
        .await?;

        // Named GCP
        let mut master_key = gcp_key.clone();
        master_key.name = Some("no_client_cert".to_string());
        provider_test(
            &client_encryption_with_names,
            master_key,
            &["SSL routines", "connection was forcibly closed"],
        )
        .await?;

        let mut master_key = gcp_key.clone();
        master_key.name = Some("with_tls".to_string());
        provider_test(
            &client_encryption_with_names,
            master_key,
            &["HTTP status=404"],
        )
        .await?;

        // Named KMIP
        let mut master_key = kmip_key.clone();
        master_key.name = Some("no_client_cert".to_string());
        provider_test(
            &client_encryption_with_names,
            master_key,
            &["SSL routines", "connection was forcibly closed"],
        )
        .await?;

        let mut master_key = kmip_key.clone();
        master_key.name = Some("with_tls".to_string());
        client_encryption_with_names
            .create_data_key(master_key)
            .await?;

        Ok(())
    }
}
