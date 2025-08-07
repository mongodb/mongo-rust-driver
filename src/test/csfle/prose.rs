use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
        Mutex,
    },
    time::Duration,
};

use futures_util::TryStreamExt;
use mongocrypt::ctx::Algorithm;
use tokio::net::TcpListener;

use crate::{
    bson::{
        doc,
        rawdoc,
        spec::ElementType,
        Binary,
        Bson,
        DateTime,
        Document,
        RawBson,
        RawDocumentBuf,
    },
    client_encryption::{
        AwsMasterKey,
        AzureMasterKey,
        ClientEncryption,
        EncryptKey,
        GcpMasterKey,
        LocalMasterKey,
        MasterKey,
        RangeOptions,
    },
    error::{ErrorKind, WriteError, WriteFailure},
    event::{
        command::{CommandFailedEvent, CommandStartedEvent, CommandSucceededEvent},
        sdam::SdamEvent,
    },
    options::{EncryptOptions, FindOptions, IndexOptions, WriteConcern},
    runtime,
    test::{
        get_client_options,
        log_uncaptured,
        server_version_lt,
        topology_is_standalone,
        util::{
            event_buffer::EventBuffer,
            fail_point::{FailPoint, FailPointMode},
        },
        Event,
        TestClient,
    },
    Client,
    Collection,
    IndexModel,
};

use super::{
    custom_endpoint_setup,
    failure,
    fle2v2_ok,
    init_client,
    load_testdata,
    validate_roundtrip,
    Result,
    AWS_KMS,
    DISABLE_CRYPT_SHARED,
    EXTRA_OPTIONS,
    KV_NAMESPACE,
    LOCAL_KMS,
};

// Prose test 1. Custom Key Material Test
#[tokio::test]
async fn custom_key_material() -> Result<()> {
    let (client, datakeys) = init_client().await?;
    let enc = ClientEncryption::new(
        client.into_client(),
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )?;

    let key = base64::decode(
        "xPTAjBRG5JiPm+d3fj6XLi2q5DMXUS/f1f+SMAlhhwkhDRL0kr8r9GDLIGTAGlvC+HVjSIgdL+RKw\
         ZCvpXSyxTICWSXTUYsWYPyu3IoHbuBZdmw2faM3WhcRIgbMReU5",
    )
    .unwrap();
    let id = enc
        .create_data_key(LocalMasterKey::builder().build())
        .key_material(key)
        .await?;
    let mut key_doc = datakeys
        .find_one(doc! { "_id": id.clone() })
        .await?
        .unwrap();
    datakeys.delete_one(doc! { "_id": id}).await?;
    let new_key_id = crate::bson::Binary::from_uuid(crate::bson::Uuid::from_bytes([0; 16]));
    key_doc.insert("_id", new_key_id.clone());
    datakeys.insert_one(key_doc).await?;

    let encrypted = enc
        .encrypt("test", EncryptKey::Id(new_key_id), Algorithm::Deterministic)
        .await?;
    let expected = base64::decode(
        "AQAAAAAAAAAAAAAAAAAAAAACz0ZOLuuhEYi807ZXTdhbqhLaS2/t9wLifJnnNYwiw79d75QYIZ6M/\
         aYC1h9nCzCjZ7pGUpAuNnkUhnIXM3PjrA==",
    )
    .unwrap();
    assert_eq!(encrypted.bytes, expected);

    Ok(())
}

// Prose test 4. BSON Size Limits and Batch Splitting
#[tokio::test]
async fn bson_size_limits() -> Result<()> {
    const STRING_LEN_2_MIB: usize = 2_097_152;

    // Setup: db initialization.
    let (client, datakeys) = init_client().await?;
    client
        .database("db")
        .create_collection("coll")
        .validator(doc! { "$jsonSchema": load_testdata("limits/limits-schema.json")? })
        .await?;
    datakeys
        .insert_one(load_testdata("limits/limits-key.json")?)
        .await?;

    // Setup: encrypted client.
    let mut opts = get_client_options().await.clone();
    let buffer = EventBuffer::<Event>::new();

    opts.command_event_handler = Some(buffer.handler());
    let client_encrypted =
        Client::encrypted_builder(opts, KV_NAMESPACE.clone(), vec![LOCAL_KMS.clone()])?
            .extra_options(EXTRA_OPTIONS.clone())
            .disable_crypt_shared(*DISABLE_CRYPT_SHARED)
            .build()
            .await?;
    let coll = client_encrypted
        .database("db")
        .collection::<Document>("coll");
    let coll2 = client_encrypted
        .database("db")
        .collection::<Document>("coll2");
    coll2.drop().await?;

    // Tests
    // Test operation 1
    coll.insert_one(doc! {
        "_id": "over_2mib_under_16mib",
        "unencrypted": "a".repeat(STRING_LEN_2_MIB),
    })
    .await?;

    // Test operation 2
    let mut doc: Document = load_testdata("limits/limits-doc.json")?;
    doc.insert("_id", "encryption_exceeds_2mib");
    doc.insert("unencrypted", "a".repeat(STRING_LEN_2_MIB - 2_000));
    coll.insert_one(doc).await?;

    // Test operation 3
    let value = "a".repeat(STRING_LEN_2_MIB);
    let mut events = buffer.stream();
    coll.insert_many(vec![
        doc! {
            "_id": "over_2mib_1",
            "unencrypted": value.clone(),
        },
        doc! {
            "_id": "over_2mib_2",
            "unencrypted": value,
        },
    ])
    .await?;
    let inserts = events
        .collect(Duration::from_millis(500), |ev| {
            let ev = match ev.as_command_started_event() {
                Some(e) => e,
                None => return false,
            };
            ev.command_name == "insert"
        })
        .await;
    assert_eq!(2, inserts.len());

    // Test operation 4
    let mut doc = load_testdata("limits/limits-doc.json")?;
    doc.insert("_id", "encryption_exceeds_2mib_1");
    doc.insert("unencrypted", "a".repeat(STRING_LEN_2_MIB - 2_000));
    let mut doc2 = doc.clone();
    doc2.insert("_id", "encryption_exceeds_2mib_2");
    let mut events = buffer.stream();
    coll.insert_many(vec![doc, doc2]).await?;
    let inserts = events
        .collect(Duration::from_millis(500), |ev| {
            let ev = match ev.as_command_started_event() {
                Some(e) => e,
                None => return false,
            };
            ev.command_name == "insert"
        })
        .await;
    assert_eq!(2, inserts.len());

    // Test operation 5
    let doc = doc! {
        "_id": "under_16mib",
        "unencrypted": "a".repeat(16_777_216 - 2_000),
    };
    coll.insert_one(doc).await?;

    // Test operation 6
    let mut doc: Document = load_testdata("limits/limits-doc.json")?;
    doc.insert("_id", "encryption_exceeds_16mib");
    doc.insert("unencrypted", "a".repeat(16_777_216 - 2_000));
    let result = coll.insert_one(doc).await;
    let err = result.unwrap_err();
    assert!(
        matches!(*err.kind, ErrorKind::Write(_)),
        "unexpected error: {err}"
    );

    // The remaining test operations use bulk_write.
    if server_version_lt(8, 0).await {
        return Ok(());
    }

    // Test operation 7
    let long_string = "a".repeat(STRING_LEN_2_MIB - 1_500);
    let write_models = vec![
        coll2.insert_one_model(doc! { "_id": "over_2mib_3", "unencrypted": &long_string })?,
        coll2.insert_one_model(doc! { "_id": "over_2mib_4", "unencrypted": &long_string })?,
    ];
    client_encrypted.bulk_write(write_models).await?;
    let bulk_write_events = buffer.get_command_started_events(&["bulkWrite"]);
    assert_eq!(bulk_write_events.len(), 2);

    // Test operation 8
    let limits: Document = load_testdata("limits/limits-qe-doc.json")?;
    let long_string = "a".repeat(STRING_LEN_2_MIB - 2_000 - 1_500);

    let mut doc1 = limits.clone();
    doc1.insert("_id", "encryption_exceeds_2mib_3");
    doc1.insert("foo", &long_string);
    let write_model1 = coll2.insert_one_model(doc1)?;

    let mut doc2 = limits;
    doc2.insert("_id", "encryption_exceeds_2mib_4");
    doc2.insert("foo", &long_string);
    let write_model2 = coll2.insert_one_model(doc2)?;

    client_encrypted
        .bulk_write(vec![write_model1, write_model2])
        .await?;
    // ignore the bulkWrite events from test operation 7
    let bulk_write_events = &buffer.get_command_started_events(&["bulkWrite"])[2..];
    assert_eq!(bulk_write_events.len(), 2);

    Ok(())
}

// Prose test 5. Views Are Prohibited
#[tokio::test]
async fn views_prohibited() -> Result<()> {
    // Setup: db initialization.
    let (client, _) = init_client().await?;
    client
        .database("db")
        .collection::<Document>("view")
        .drop()
        .await?;
    client
        .database("db")
        .create_collection("view")
        .view_on("coll".to_string())
        .await?;

    // Setup: encrypted client.
    let client_encrypted = Client::encrypted_builder(
        get_client_options().await.clone(),
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )?
    .extra_options(EXTRA_OPTIONS.clone())
    .disable_crypt_shared(*DISABLE_CRYPT_SHARED)
    .build()
    .await?;

    // Test: auto encryption fails on a view
    let result = client_encrypted
        .database("db")
        .collection::<Document>("view")
        .insert_one(doc! {})
        .await;
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("cannot auto encrypt a view"),
        "unexpected error: {err}"
    );

    Ok(())
}

// Prose test 7. Custom Endpoint
mod custom_endpoint {
    use crate::client_encryption::KmipMasterKey;

    use super::*;

    async fn custom_endpoint_aws_ok(endpoint: Option<String>) -> Result<()> {
        let client_encryption = custom_endpoint_setup(true).await?;

        let key_id = client_encryption
            .create_data_key(
                AwsMasterKey::builder()
                    .region("us-east-1")
                    .key(
                        "arn:aws:kms:us-east-1:579766882180:key/\
                         89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
                    )
                    .endpoint(endpoint)
                    .build(),
            )
            .await?;
        validate_roundtrip(&client_encryption, key_id).await?;

        Ok(())
    }

    // case 1
    #[tokio::test]
    async fn aws_no_endpoint() -> Result<()> {
        custom_endpoint_aws_ok(None).await
    }

    // case 2
    #[tokio::test]
    async fn aws_no_port() -> Result<()> {
        custom_endpoint_aws_ok(Some("kms.us-east-1.amazonaws.com".to_string())).await
    }

    // case 3
    #[tokio::test]
    async fn aws_with_port() -> Result<()> {
        custom_endpoint_aws_ok(Some("kms.us-east-1.amazonaws.com:443".to_string())).await
    }

    // case 4
    #[tokio::test]
    async fn kmip_invalid_port() -> Result<()> {
        let client_encryption = custom_endpoint_setup(true).await?;

        let result = client_encryption
            .create_data_key(
                KmipMasterKey::builder()
                    .key_id("1".to_owned())
                    .endpoint("localhost:12345".to_owned())
                    .build(),
            )
            .await;
        assert!(result.unwrap_err().is_network_error());

        Ok(())
    }

    // case 5
    #[tokio::test]
    async fn aws_invalid_region() -> Result<()> {
        let client_encryption = custom_endpoint_setup(true).await?;

        let result = client_encryption
            .create_data_key(
                AwsMasterKey::builder()
                    .region("us-east-1")
                    .key(
                        "arn:aws:kms:us-east-1:579766882180:key/\
                         89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
                    )
                    .endpoint(Some("kms.us-east-2.amazonaws.com".to_string()))
                    .build(),
            )
            .await;
        assert!(result.unwrap_err().is_csfle_error());

        Ok(())
    }

    // case 6
    #[tokio::test]
    async fn aws_invalid_domain() -> Result<()> {
        let client_encryption = custom_endpoint_setup(true).await?;

        let result = client_encryption
            .create_data_key(
                AwsMasterKey::builder()
                    .region("us-east-1")
                    .key(
                        "arn:aws:kms:us-east-1:579766882180:key/\
                         89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
                    )
                    .endpoint(Some("doesnotexist.invalid".to_string()))
                    .build(),
            )
            .await;
        assert!(result.unwrap_err().is_network_error());

        Ok(())
    }

    // case 7
    #[tokio::test]
    async fn azure() -> Result<()> {
        let master_key = AzureMasterKey::builder()
            .key_vault_endpoint("key-vault-csfle.vault.azure.net")
            .key_name("key-name-csfle")
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

    // case 8
    #[tokio::test]
    async fn gcp_valid() -> Result<()> {
        let master_key = GcpMasterKey::builder()
            .project_id("devprod-drivers")
            .location("global")
            .key_ring("key-ring-csfle")
            .key_name("key-name-csfle")
            .endpoint(Some("cloudkms.googleapis.com:443".to_string()))
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

    // case 9
    #[tokio::test]
    async fn gcp_invalid() -> Result<()> {
        let master_key = GcpMasterKey::builder()
            .project_id("devprod-drivers")
            .location("global")
            .key_ring("key-ring-csfle")
            .key_name("key-name-csfle")
            .endpoint(Some("doesnotexist.invalid:443".to_string()))
            .build();

        let client_encryption = custom_endpoint_setup(true).await?;
        let result = client_encryption.create_data_key(master_key).await;
        let err = result.unwrap_err();
        assert!(err.is_csfle_error());
        assert!(
            err.to_string().contains("Invalid KMS response"),
            "unexpected error: {err}"
        );

        Ok(())
    }

    // case 10
    #[cfg(feature = "openssl-tls")]
    #[tokio::test]
    async fn kmip_valid() -> Result<()> {
        let master_key = KmipMasterKey::builder().key_id("1".to_owned()).build();

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

    // case 11
    #[cfg(feature = "openssl-tls")]
    #[tokio::test]
    async fn kmip_valid_endpoint() -> Result<()> {
        let master_key = KmipMasterKey::builder()
            .key_id("1".to_owned())
            .endpoint("localhost:5698".to_owned())
            .build();

        let client_encryption = custom_endpoint_setup(true).await?;
        let key_id = client_encryption
            .create_data_key(master_key.clone())
            .await?;
        validate_roundtrip(&client_encryption, key_id).await?;

        Ok(())
    }

    // case 12
    #[tokio::test]
    async fn kmip_invalid() -> Result<()> {
        let master_key = KmipMasterKey::builder()
            .key_id("1".to_owned())
            .endpoint("doesnotexist.invalid:5698".to_owned())
            .build();

        let client_encryption = custom_endpoint_setup(true).await?;
        let result = client_encryption.create_data_key(master_key).await;
        let err = result.unwrap_err();
        assert!(err.is_network_error());

        Ok(())
    }
}

// Prose test 8. Bypass Spawning mongocryptd
mod bypass_spawning_mongocryptd {
    use super::*;

    async fn bypass_mongocryptd_unencrypted_insert(bypass: Bypass) -> Result<()> {
        // Setup: encrypted client.
        let extra_options = doc! {
            "mongocryptdSpawnArgs": [ "--pidfilepath=bypass-spawning-mongocryptd.pid", "--port=27021"],
        };
        let builder = Client::encrypted_builder(
            get_client_options().await.clone(),
            KV_NAMESPACE.clone(),
            vec![LOCAL_KMS.clone()],
        )?
        .extra_options(extra_options)
        .disable_crypt_shared(true);
        let builder = match bypass {
            Bypass::AutoEncryption => builder.bypass_auto_encryption(true),
            Bypass::QueryAnalysis => builder.bypass_query_analysis(true),
        };
        let client_encrypted = builder.build().await?;

        // Test: insert succeeds.
        client_encrypted
            .database("db")
            .collection::<Document>("coll")
            .insert_one(doc! { "unencrypted": "test" })
            .await?;
        // Test: mongocryptd not spawned.
        assert!(!client_encrypted.mongocryptd_spawned().await);
        // Test: attempting to connect fails.
        let client =
            Client::with_uri_str("mongodb://localhost:27021/?serverSelectionTimeoutMS=1000")
                .await?;
        let result = client.list_database_names().await;
        assert!(result.unwrap_err().is_server_selection_error());

        Ok(())
    }

    enum Bypass {
        AutoEncryption,
        QueryAnalysis,
    }

    #[tokio::test]
    async fn shared_library() -> Result<()> {
        if *DISABLE_CRYPT_SHARED {
            log_uncaptured(
                "Skipping bypass mongocryptd via shared library test: crypt_shared is disabled.",
            );
            return Ok(());
        }

        // Setup: encrypted client.
        let client_encrypted = Client::encrypted_builder(
            get_client_options().await.clone(),
            KV_NAMESPACE.clone(),
            vec![LOCAL_KMS.clone()],
        )?
        .schema_map([("db.coll", load_testdata("external/external-schema.json")?)])
        .extra_options(doc! {
            "mongocryptdURI": "mongodb://localhost:27021/db?serverSelectionTimeoutMS=1000",
            "mongocryptdSpawnArgs": ["--pidfilepath=bypass-spawning-mongocryptd.pid", "--port=27021"],
            "cryptSharedLibPath": EXTRA_OPTIONS.get("cryptSharedLibPath").unwrap(),
            "cryptSharedRequired": true,
        })
        .build()
        .await?;

        // Test: insert succeeds.
        client_encrypted
            .database("db")
            .collection::<Document>("coll")
            .insert_one(doc! { "unencrypted": "test" })
            .await?;
        // Test: mongocryptd not spawned.
        assert!(!client_encrypted.mongocryptd_spawned().await);
        // Test: attempting to connect fails.
        let client =
            Client::with_uri_str("mongodb://localhost:27021/?serverSelectionTimeoutMS=1000")
                .await?;
        let result = client.list_database_names().await;
        assert!(result.unwrap_err().is_server_selection_error());

        Ok(())
    }

    #[tokio::test]
    async fn bypass_spawn() -> Result<()> {
        // Setup: encrypted client.
        let extra_options = doc! {
            "mongocryptdBypassSpawn": true,
            "mongocryptdURI": "mongodb://localhost:27021/db?serverSelectionTimeoutMS=1000",
            "mongocryptdSpawnArgs": [ "--pidfilepath=bypass-spawning-mongocryptd.pid", "--port=27021"],
        };
        let client_encrypted = Client::encrypted_builder(
            get_client_options().await.clone(),
            KV_NAMESPACE.clone(),
            vec![LOCAL_KMS.clone()],
        )?
        .schema_map([("db.coll", load_testdata("external/external-schema.json")?)])
        .extra_options(extra_options)
        .disable_crypt_shared(true)
        .build()
        .await?;

        // Test: insert fails.
        let err = client_encrypted
            .database("db")
            .collection::<Document>("coll")
            .insert_one(doc! { "encrypted": "test" })
            .await
            .unwrap_err();
        assert!(err.is_server_selection_error(), "unexpected error: {err}");

        Ok(())
    }

    #[tokio::test]
    async fn auto_encryption() -> Result<()> {
        bypass_mongocryptd_unencrypted_insert(Bypass::AutoEncryption).await
    }

    #[tokio::test]
    async fn bypass_query_analysis() -> Result<()> {
        bypass_mongocryptd_unencrypted_insert(Bypass::QueryAnalysis).await
    }
}

// Prose test 9. Deadlock Tests
mod deadlock {
    use super::*;

    struct DeadlockTestCase {
        max_pool_size: u32,
        bypass_auto_encryption: bool,
        set_key_vault_client: bool,
        expected_encrypted_commands: Vec<DeadlockExpectation>,
        expected_keyvault_commands: Vec<DeadlockExpectation>,
        expected_number_of_clients: usize,
    }

    impl DeadlockTestCase {
        async fn run(&self) -> Result<()> {
            // Setup
            let client_test = Client::for_test().await;
            let client_keyvault = Client::for_test()
                .options({
                    let mut opts = get_client_options().await.clone();
                    opts.max_pool_size = Some(1);
                    opts
                })
                .monitor_events()
                .await;

            let mut keyvault_events = client_keyvault.events.stream();
            client_test
                .database("keyvault")
                .collection::<Document>("datakeys")
                .drop()
                .await?;
            client_test
                .database("db")
                .collection::<Document>("coll")
                .drop()
                .await?;
            client_keyvault
                .database("keyvault")
                .collection::<Document>("datakeys")
                .insert_one(load_testdata("external/external-key.json")?)
                .write_concern(WriteConcern::majority())
                .await?;
            client_test
                .database("db")
                .create_collection("coll")
                .validator(doc! { "$jsonSchema": load_testdata("external/external-schema.json")? })
                .await?;
            let client_encryption = ClientEncryption::new(
                client_test.clone().into_client(),
                KV_NAMESPACE.clone(),
                vec![LOCAL_KMS.clone()],
            )?;
            let ciphertext = client_encryption
                .encrypt(
                    RawBson::String("string0".to_string()),
                    EncryptKey::AltName("local".to_string()),
                    Algorithm::Deterministic,
                )
                .await?;

            // Run test case
            let event_buffer = EventBuffer::new();

            let mut encrypted_events = event_buffer.stream();
            let mut opts = get_client_options().await.clone();
            opts.max_pool_size = Some(self.max_pool_size);
            opts.command_event_handler = Some(event_buffer.handler());
            opts.sdam_event_handler = Some(event_buffer.handler());
            let client_encrypted =
                Client::encrypted_builder(opts, KV_NAMESPACE.clone(), vec![LOCAL_KMS.clone()])?
                    .bypass_auto_encryption(self.bypass_auto_encryption)
                    .key_vault_client(
                        if self.set_key_vault_client {
                            Some(client_keyvault.clone().into_client())
                        } else {
                            None
                        },
                    )
                    .extra_options(EXTRA_OPTIONS.clone())
                    .disable_crypt_shared(*DISABLE_CRYPT_SHARED)
                    .build()
                    .await?;

            if self.bypass_auto_encryption {
                client_test
                    .database("db")
                    .collection::<Document>("coll")
                    .insert_one(doc! { "_id": 0, "encrypted": ciphertext })
                    .await?;
            } else {
                client_encrypted
                    .database("db")
                    .collection::<Document>("coll")
                    .insert_one(doc! { "_id": 0, "encrypted": "string0" })
                    .await?;
            }

            let found = client_encrypted
                .database("db")
                .collection::<Document>("coll")
                .find_one(doc! { "_id": 0 })
                .await?;
            assert_eq!(found, Some(doc! { "_id": 0, "encrypted": "string0" }));

            let encrypted_events = encrypted_events
                .collect(Duration::from_millis(500), |_| true)
                .await;
            let client_count = encrypted_events
                .iter()
                .filter(|ev| matches!(ev, Event::Sdam(SdamEvent::TopologyOpening(_))))
                .count();
            assert_eq!(self.expected_number_of_clients, client_count);

            let encrypted_commands: Vec<_> = encrypted_events
                .into_iter()
                .filter_map(|ev| ev.into_command_started_event())
                .collect();
            for expected in &self.expected_encrypted_commands {
                expected.assert_matches_any("encrypted", &encrypted_commands);
            }

            let keyvault_commands = keyvault_events
                .collect_map(Duration::from_millis(500), |ev| {
                    ev.into_command_started_event()
                })
                .await;
            for expected in &self.expected_keyvault_commands {
                expected.assert_matches_any("keyvault", &keyvault_commands);
            }

            Ok(())
        }
    }

    #[derive(Debug)]
    struct DeadlockExpectation {
        command: &'static str,
        db: &'static str,
    }

    impl DeadlockExpectation {
        fn matches(&self, ev: &CommandStartedEvent) -> bool {
            ev.command_name == self.command && ev.db == self.db
        }

        fn assert_matches_any(&self, name: &str, commands: &[CommandStartedEvent]) {
            for actual in commands {
                if self.matches(actual) {
                    return;
                }
            }
            panic!("No {name} command matching {self:?} found, events=\n{commands:?}");
        }
    }

    #[tokio::test]
    async fn deadlock() -> Result<()> {
        // Case 1
        DeadlockTestCase {
            max_pool_size: 1,
            bypass_auto_encryption: false,
            set_key_vault_client: false,
            expected_encrypted_commands: vec![
                DeadlockExpectation {
                    command: "listCollections",
                    db: "db",
                },
                DeadlockExpectation {
                    command: "find",
                    db: "keyvault",
                },
                DeadlockExpectation {
                    command: "insert",
                    db: "db",
                },
                DeadlockExpectation {
                    command: "find",
                    db: "db",
                },
            ],
            expected_keyvault_commands: vec![],
            expected_number_of_clients: 2,
        }
        .run()
        .await?;
        // Case 2
        DeadlockTestCase {
            max_pool_size: 1,
            bypass_auto_encryption: false,
            set_key_vault_client: true,
            expected_encrypted_commands: vec![
                DeadlockExpectation {
                    command: "listCollections",
                    db: "db",
                },
                DeadlockExpectation {
                    command: "insert",
                    db: "db",
                },
                DeadlockExpectation {
                    command: "find",
                    db: "db",
                },
            ],
            expected_keyvault_commands: vec![DeadlockExpectation {
                command: "find",
                db: "keyvault",
            }],
            expected_number_of_clients: 2,
        }
        .run()
        .await?;
        // Case 3
        DeadlockTestCase {
            max_pool_size: 1,
            bypass_auto_encryption: true,
            set_key_vault_client: false,
            expected_encrypted_commands: vec![
                DeadlockExpectation {
                    command: "find",
                    db: "db",
                },
                DeadlockExpectation {
                    command: "find",
                    db: "keyvault",
                },
            ],
            expected_keyvault_commands: vec![],
            expected_number_of_clients: 2,
        }
        .run()
        .await?;
        // Case 4
        DeadlockTestCase {
            max_pool_size: 1,
            bypass_auto_encryption: true,
            set_key_vault_client: true,
            expected_encrypted_commands: vec![DeadlockExpectation {
                command: "find",
                db: "db",
            }],
            expected_keyvault_commands: vec![DeadlockExpectation {
                command: "find",
                db: "keyvault",
            }],
            expected_number_of_clients: 1,
        }
        .run()
        .await?;
        // Case 5: skipped (unlimited max_pool_size not supported)
        // Case 6: skipped (unlimited max_pool_size not supported)
        // Case 7: skipped (unlimited max_pool_size not supported)
        // Case 8: skipped (unlimited max_pool_size not supported)

        Ok(())
    }
}

// Prose test 12. Explicit Encryption
mod explicit_encryption {
    use super::*;

    struct ExplicitEncryptionTestData {
        key1_id: Binary,
        client_encryption: ClientEncryption,
        encrypted_client: Client,
    }

    async fn explicit_encryption_setup() -> Result<Option<ExplicitEncryptionTestData>> {
        if server_version_lt(6, 0).await {
            log_uncaptured("skipping explicit encryption test: server below 6.0");
            return Ok(None);
        }
        if topology_is_standalone().await {
            log_uncaptured("skipping explicit encryption test: cannot run on standalone");
            return Ok(None);
        }

        let key_vault_client = Client::for_test().await;

        let encrypted_fields = load_testdata("data/encryptedFields.json")?;
        let key1_document = load_testdata("data/keys/key1-document.json")?;
        let key1_id = match key1_document.get("_id").unwrap() {
            Bson::Binary(b) => b.clone(),
            v => return Err(failure!("expected binary _id, got {:?}", v)),
        };

        let db = key_vault_client.database("db");
        db.collection::<Document>("explicit_encryption")
            .drop()
            .encrypted_fields(encrypted_fields.clone())
            .await?;
        db.create_collection("explicit_encryption")
            .encrypted_fields(encrypted_fields)
            .await?;
        let keyvault = key_vault_client.database("keyvault");
        keyvault.collection::<Document>("datakeys").drop().await?;
        keyvault.create_collection("datakeys").await?;
        keyvault
            .collection::<Document>("datakeys")
            .insert_one(key1_document)
            .write_concern(WriteConcern::majority())
            .await?;

        let client_encryption = ClientEncryption::new(
            key_vault_client.into_client(),
            KV_NAMESPACE.clone(),
            vec![LOCAL_KMS.clone()],
        )?;
        let encrypted_client = Client::encrypted_builder(
            get_client_options().await.clone(),
            KV_NAMESPACE.clone(),
            vec![LOCAL_KMS.clone()],
        )?
        .bypass_query_analysis(true)
        .extra_options(EXTRA_OPTIONS.clone())
        .disable_crypt_shared(*DISABLE_CRYPT_SHARED)
        .build()
        .await?;

        Ok(Some(ExplicitEncryptionTestData {
            key1_id,
            client_encryption,
            encrypted_client,
        }))
    }

    // can insert encrypted indexed and find
    #[tokio::test]
    async fn case_1() -> Result<()> {
        if !fle2v2_ok("explicit_encryption_case_1").await {
            return Ok(());
        }

        let testdata = match explicit_encryption_setup().await? {
            Some(t) => t,
            None => return Ok(()),
        };
        let enc_coll = testdata
            .encrypted_client
            .database("db")
            .collection::<Document>("explicit_encryption");

        let insert_payload = testdata
            .client_encryption
            .encrypt(
                "encrypted indexed value",
                EncryptKey::Id(testdata.key1_id.clone()),
                Algorithm::Indexed,
            )
            .contention_factor(0)
            .await?;
        enc_coll
            .insert_one(doc! { "encryptedIndexed": insert_payload })
            .await?;

        let find_payload = testdata
            .client_encryption
            .encrypt(
                "encrypted indexed value",
                EncryptKey::Id(testdata.key1_id),
                Algorithm::Indexed,
            )
            .query_type("equality".to_string())
            .contention_factor(0)
            .await?;
        let found: Vec<_> = enc_coll
            .find(doc! { "encryptedIndexed": find_payload })
            .await?
            .try_collect()
            .await?;
        assert_eq!(1, found.len());
        assert_eq!(
            "encrypted indexed value",
            found[0].get_str("encryptedIndexed")?
        );

        Ok(())
    }

    // can insert encrypted indexed and find with non-zero contention
    #[tokio::test]
    async fn case_2() -> Result<()> {
        if !fle2v2_ok("explicit_encryption_case_2").await {
            return Ok(());
        }

        let testdata = match explicit_encryption_setup().await? {
            Some(t) => t,
            None => return Ok(()),
        };
        let enc_coll = testdata
            .encrypted_client
            .database("db")
            .collection::<Document>("explicit_encryption");

        for _ in 0..10 {
            let insert_payload = testdata
                .client_encryption
                .encrypt(
                    "encrypted indexed value",
                    EncryptKey::Id(testdata.key1_id.clone()),
                    Algorithm::Indexed,
                )
                .contention_factor(10)
                .await?;
            enc_coll
                .insert_one(doc! { "encryptedIndexed": insert_payload })
                .await?;
        }

        let find_payload = testdata
            .client_encryption
            .encrypt(
                "encrypted indexed value",
                EncryptKey::Id(testdata.key1_id.clone()),
                Algorithm::Indexed,
            )
            .query_type("equality".to_string())
            .contention_factor(0)
            .await?;
        let found: Vec<_> = enc_coll
            .find(doc! { "encryptedIndexed": find_payload })
            .await?
            .try_collect()
            .await?;
        assert!(found.len() < 10);
        for doc in found {
            assert_eq!("encrypted indexed value", doc.get_str("encryptedIndexed")?);
        }

        let find_payload2 = testdata
            .client_encryption
            .encrypt(
                "encrypted indexed value",
                EncryptKey::Id(testdata.key1_id.clone()),
                Algorithm::Indexed,
            )
            .query_type("equality")
            .contention_factor(10)
            .await?;
        let found: Vec<_> = enc_coll
            .find(doc! { "encryptedIndexed": find_payload2 })
            .await?
            .try_collect()
            .await?;
        assert_eq!(10, found.len());
        for doc in found {
            assert_eq!("encrypted indexed value", doc.get_str("encryptedIndexed")?);
        }

        Ok(())
    }

    // can insert encrypted unindexed
    #[tokio::test]
    async fn case_3() -> Result<()> {
        if !fle2v2_ok("explicit_encryption_case_3").await {
            return Ok(());
        }

        let testdata = match explicit_encryption_setup().await? {
            Some(t) => t,
            None => return Ok(()),
        };
        let enc_coll = testdata
            .encrypted_client
            .database("db")
            .collection::<Document>("explicit_encryption");

        let insert_payload = testdata
            .client_encryption
            .encrypt(
                "encrypted unindexed value",
                EncryptKey::Id(testdata.key1_id.clone()),
                Algorithm::Unindexed,
            )
            .await?;
        enc_coll
            .insert_one(doc! { "_id": 1, "encryptedUnindexed": insert_payload })
            .await?;

        let found: Vec<_> = enc_coll
            .find(doc! { "_id": 1 })
            .await?
            .try_collect()
            .await?;
        assert_eq!(1, found.len());
        assert_eq!(
            "encrypted unindexed value",
            found[0].get_str("encryptedUnindexed")?
        );

        Ok(())
    }

    // can roundtrip encrypted indexed
    #[tokio::test]
    async fn case_4() -> Result<()> {
        if !fle2v2_ok("explicit_encryption_case_4").await {
            return Ok(());
        }

        let testdata = match explicit_encryption_setup().await? {
            Some(t) => t,
            None => return Ok(()),
        };

        let raw_value = RawBson::String("encrypted indexed value".to_string());
        let payload = testdata
            .client_encryption
            .encrypt(
                raw_value.clone(),
                EncryptKey::Id(testdata.key1_id.clone()),
                Algorithm::Indexed,
            )
            .contention_factor(0)
            .await?;
        let roundtrip = testdata
            .client_encryption
            .decrypt(payload.as_raw_binary())
            .await?;
        assert_eq!(raw_value, roundtrip);

        Ok(())
    }

    //can roundtrip encrypted unindexed)
    #[tokio::test]
    async fn case_5() -> Result<()> {
        if !fle2v2_ok("explicit_encryption_case_5").await {
            return Ok(());
        }

        let testdata = match explicit_encryption_setup().await? {
            Some(t) => t,
            None => return Ok(()),
        };

        let raw_value = RawBson::String("encrypted unindexed value".to_string());
        let payload = testdata
            .client_encryption
            .encrypt(
                raw_value.clone(),
                EncryptKey::Id(testdata.key1_id.clone()),
                Algorithm::Unindexed,
            )
            .await?;
        let roundtrip = testdata
            .client_encryption
            .decrypt(payload.as_raw_binary())
            .await?;
        assert_eq!(raw_value, roundtrip);

        Ok(())
    }
}

// Prose test 13. Unique Index on keyAltNames
mod unique_index_on_key_alt_names {
    use super::*;

    async fn unique_index_keyaltnames_setup() -> Result<(ClientEncryption, Binary)> {
        let client = Client::for_test().await;
        let datakeys = client
            .database("keyvault")
            .collection::<Document>("datakeys");
        datakeys.drop().await?;
        datakeys
            .create_index(IndexModel {
                keys: doc! { "keyAltNames": 1 },
                options: Some(
                    IndexOptions::builder()
                        .name("keyAltNames_1".to_string())
                        .unique(true)
                        .partial_filter_expression(doc! { "keyAltNames": { "$exists": true } })
                        .build(),
                ),
            })
            .write_concern(WriteConcern::majority())
            .await?;
        let client_encryption = ClientEncryption::new(
            client.into_client(),
            KV_NAMESPACE.clone(),
            vec![LOCAL_KMS.clone()],
        )?;
        let key = client_encryption
            .create_data_key(LocalMasterKey::builder().build())
            .key_alt_names(vec!["def".to_string()])
            .await?;
        Ok((client_encryption, key))
    }

    // `Error::code` skips write errors per the SDAM spec, but we need those.
    fn write_err_code(err: &crate::error::Error) -> Option<i32> {
        if let Some(code) = err.sdam_code() {
            return Some(code);
        }
        match *err.kind {
            ErrorKind::Write(WriteFailure::WriteError(WriteError { code, .. })) => Some(code),
            _ => None,
        }
    }

    // createDataKey
    #[tokio::test]
    async fn case_1() -> Result<()> {
        let (client_encryption, _) = unique_index_keyaltnames_setup().await?;

        // Succeeds
        client_encryption
            .create_data_key(LocalMasterKey::builder().build())
            .key_alt_names(vec!["abc".to_string()])
            .await?;
        // Fails: duplicate key
        let err = client_encryption
            .create_data_key(LocalMasterKey::builder().build())
            .key_alt_names(vec!["abc".to_string()])
            .await
            .unwrap_err();
        assert_eq!(Some(11000), write_err_code(&err), "unexpected error: {err}");
        // Fails: duplicate key
        let err = client_encryption
            .create_data_key(LocalMasterKey::builder().build())
            .key_alt_names(vec!["def".to_string()])
            .await
            .unwrap_err();
        assert_eq!(Some(11000), write_err_code(&err), "unexpected error: {err}");

        Ok(())
    }

    // add_key_alt_name
    #[tokio::test]
    async fn case_2() -> Result<()> {
        let (client_encryption, key) = unique_index_keyaltnames_setup().await?;

        // Succeeds
        let new_key = client_encryption
            .create_data_key(LocalMasterKey::builder().build())
            .await?;
        client_encryption.add_key_alt_name(&new_key, "abc").await?;
        // Still succeeds, has alt name
        let prev_key = client_encryption
            .add_key_alt_name(&new_key, "abc")
            .await?
            .unwrap();
        assert_eq!("abc", prev_key.get_array("keyAltNames")?.get_str(0)?);
        // Fails: adding alt name used for `key` to `new_key`
        let err = client_encryption
            .add_key_alt_name(&new_key, "def")
            .await
            .unwrap_err();
        assert_eq!(Some(11000), write_err_code(&err), "unexpected error: {err}");
        // Succeds: re-adding alt name to `new_key`
        let prev_key = client_encryption
            .add_key_alt_name(&key, "def")
            .await?
            .unwrap();
        assert_eq!("def", prev_key.get_array("keyAltNames")?.get_str(0)?);

        Ok(())
    }
}

// Prose test 14. Decryption Events
mod decryption_events {
    use super::*;

    struct DecryptionEventsTestdata {
        setup_client: TestClient,
        decryption_events: Collection<Document>,
        ev_handler: Arc<DecryptionEventsHandler>,
        ciphertext: Binary,
        malformed_ciphertext: Binary,
    }

    impl DecryptionEventsTestdata {
        async fn setup() -> Result<Option<Self>> {
            if !topology_is_standalone().await {
                log_uncaptured("skipping decryption events test: requires standalone topology");
                return Ok(None);
            }

            let setup_client = Client::for_test().await;
            let db = setup_client.database("db");
            db.collection::<Document>("decryption_events")
                .drop()
                .await?;
            db.create_collection("decryption_events").await?;

            let client_encryption = ClientEncryption::new(
                setup_client.clone().into_client(),
                KV_NAMESPACE.clone(),
                vec![LOCAL_KMS.clone()],
            )?;
            let key_id = client_encryption
                .create_data_key(LocalMasterKey::builder().build())
                .await?;
            let ciphertext = client_encryption
                .encrypt("hello", EncryptKey::Id(key_id), Algorithm::Deterministic)
                .await?;
            let mut malformed_ciphertext = ciphertext.clone();
            let last = malformed_ciphertext.bytes.last_mut().unwrap();
            *last = last.wrapping_add(1);

            let ev_handler = DecryptionEventsHandler::new();
            let mut opts = get_client_options().await.clone();
            opts.retry_reads = Some(false);
            opts.command_event_handler = Some(ev_handler.clone().into());
            let encrypted_client =
                Client::encrypted_builder(opts, KV_NAMESPACE.clone(), vec![LOCAL_KMS.clone()])?
                    .extra_options(EXTRA_OPTIONS.clone())
                    .disable_crypt_shared(*DISABLE_CRYPT_SHARED)
                    .build()
                    .await?;
            let decryption_events = encrypted_client
                .database("db")
                .collection("decryption_events");

            Ok(Some(Self {
                setup_client,
                decryption_events,
                ev_handler,
                ciphertext,
                malformed_ciphertext,
            }))
        }
    }

    #[derive(Debug)]
    struct DecryptionEventsHandler {
        succeeded: Mutex<Option<CommandSucceededEvent>>,
        failed: Mutex<Option<CommandFailedEvent>>,
    }

    impl DecryptionEventsHandler {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                succeeded: Mutex::new(None),
                failed: Mutex::new(None),
            })
        }
    }

    #[allow(deprecated)]
    impl crate::event::command::CommandEventHandler for DecryptionEventsHandler {
        fn handle_command_succeeded_event(&self, event: CommandSucceededEvent) {
            if event.command_name == "aggregate" {
                *self.succeeded.lock().unwrap() = Some(event);
            }
        }

        fn handle_command_failed_event(&self, event: CommandFailedEvent) {
            if event.command_name == "aggregate" {
                *self.failed.lock().unwrap() = Some(event);
            }
        }
    }

    // command error
    #[tokio::test(flavor = "multi_thread")]
    async fn case_1() -> Result<()> {
        let td = match DecryptionEventsTestdata::setup().await? {
            Some(v) => v,
            None => return Ok(()),
        };

        let fail_point =
            FailPoint::fail_command(&["aggregate"], FailPointMode::Times(1)).error_code(123);
        let _guard = td.setup_client.enable_fail_point(fail_point).await.unwrap();
        let err = td
            .decryption_events
            .aggregate(vec![doc! { "$count": "total" }])
            .await
            .unwrap_err();
        assert_eq!(Some(123), err.sdam_code());
        assert!(td.ev_handler.failed.lock().unwrap().is_some());

        Ok(())
    }

    // network error
    #[tokio::test(flavor = "multi_thread")]
    async fn case_2() -> Result<()> {
        let td = match DecryptionEventsTestdata::setup().await? {
            Some(v) => v,
            None => return Ok(()),
        };

        let fail_point = FailPoint::fail_command(&["aggregate"], FailPointMode::Times(1))
            .error_code(123)
            .close_connection(true);
        let _guard = td.setup_client.enable_fail_point(fail_point).await.unwrap();
        let err = td
            .decryption_events
            .aggregate(vec![doc! { "$count": "total" }])
            .await
            .unwrap_err();
        assert!(err.is_network_error(), "unexpected error: {err}");
        assert!(td.ev_handler.failed.lock().unwrap().is_some());

        Ok(())
    }

    // decrypt error
    #[tokio::test]
    async fn case_3() -> Result<()> {
        let td = match DecryptionEventsTestdata::setup().await? {
            Some(v) => v,
            None => return Ok(()),
        };
        td.decryption_events
            .insert_one(doc! { "encrypted": td.malformed_ciphertext })
            .await?;
        let err = td.decryption_events.aggregate(vec![]).await.unwrap_err();
        assert!(err.is_csfle_error());
        let guard = td.ev_handler.succeeded.lock().unwrap();
        let ev = guard.as_ref().unwrap();
        assert_eq!(
            ElementType::Binary,
            ev.reply.get_document("cursor")?.get_array("firstBatch")?[0]
                .as_document()
                .unwrap()
                .get("encrypted")
                .unwrap()
                .element_type()
        );

        Ok(())
    }

    // decrypt success
    #[tokio::test]
    async fn case_4() -> Result<()> {
        let td = match DecryptionEventsTestdata::setup().await? {
            Some(v) => v,
            None => return Ok(()),
        };
        td.decryption_events
            .insert_one(doc! { "encrypted": td.ciphertext })
            .await?;
        td.decryption_events.aggregate(vec![]).await?;
        let guard = td.ev_handler.succeeded.lock().unwrap();
        let ev = guard.as_ref().unwrap();
        assert_eq!(
            ElementType::Binary,
            ev.reply.get_document("cursor")?.get_array("firstBatch")?[0]
                .as_document()
                .unwrap()
                .get("encrypted")
                .unwrap()
                .element_type()
        );

        Ok(())
    }
}

// TODO RUST-1441: implement prose test 16. Rewrap

// Prose test 19. Azure IMDS Credentials Integration Test (case 1: failure)
#[cfg(feature = "azure-kms")]
#[tokio::test]
async fn azure_imds_integration_failure() -> Result<()> {
    use mongocrypt::ctx::KmsProvider;

    let c = ClientEncryption::new(
        Client::for_test().await.into_client(),
        KV_NAMESPACE.clone(),
        [(KmsProvider::azure(), doc! {}, None)],
    )?;

    let result = c
        .create_data_key(
            AzureMasterKey::builder()
                .key_vault_endpoint("https://keyvault-drivers-2411.vault.azure.net/keys/")
                .key_name("KEY-NAME")
                .build(),
        )
        .await;

    assert!(result.is_err(), "expected error, got {result:?}");
    assert!(result.unwrap_err().is_auth_error());

    Ok(())
}

// Prose test 20. Bypass creating mongocryptd client when shared library is loaded
#[tokio::test]
async fn bypass_mongocryptd_client() -> Result<()> {
    if *DISABLE_CRYPT_SHARED {
        log_uncaptured("Skipping bypass mongocryptd client test: crypt_shared is disabled.");
        return Ok(());
    }

    async fn bind(addr: &str) -> Result<TcpListener> {
        Ok(TcpListener::bind(addr.parse::<std::net::SocketAddr>()?).await?)
    }

    let connected = Arc::new(AtomicBool::new(false));
    {
        let connected = Arc::clone(&connected);
        let listener = bind("127.0.0.1:27021").await?;
        runtime::spawn(async move {
            let _ = listener.accept().await;
            log_uncaptured("test failure: connection accepted");
            connected.store(true, Ordering::SeqCst);
        })
    };

    let client_encrypted = Client::encrypted_builder(
        get_client_options().await.clone(),
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )?
    .extra_options({
        let mut extra_options = EXTRA_OPTIONS.clone();
        extra_options.insert("mongocryptdURI", "mongodb://localhost:27021");
        extra_options
    })
    .build()
    .await?;
    client_encrypted
        .database("db")
        .collection::<Document>("coll")
        .insert_one(doc! { "unencrypted": "test" })
        .await?;

    assert!(!client_encrypted.has_mongocryptd_client().await);
    assert!(!connected.load(Ordering::SeqCst));

    Ok(())
}

// Prose test 21. Automatic Data Encryption Keys
mod auto_encryption_keys {
    use super::*;

    async fn auto_encryption_keys(master_key: impl Into<MasterKey>) -> Result<()> {
        if !fle2v2_ok("auto_encryption_keys").await {
            return Ok(());
        }

        let master_key = master_key.into();

        let client = Client::for_test().await;
        let db = client.database("test_auto_encryption_keys");
        db.drop().await?;
        let ce = ClientEncryption::new(
            client.into_client(),
            KV_NAMESPACE.clone(),
            vec![AWS_KMS.clone(), LOCAL_KMS.clone()],
        )?;

        // Case 1: Simple Creation and Validation
        ce.create_encrypted_collection(&db, "case_1", master_key.clone())
            .encrypted_fields(doc! {
                "fields": [{
                    "path": "ssn",
                    "bsonType": "string",
                    "keyId": Bson::Null,
                }],
            })
            .await
            .1?;
        let coll = db.collection::<Document>("case_1");
        let result = coll.insert_one(doc! { "ssn": "123-45-6789" }).await;
        assert!(
            result.as_ref().unwrap_err().code() == Some(121),
            "Expected error 121 (failed validation), got {result:?}"
        );

        // Case 2: Missing encryptedFields
        let result = ce
            .create_encrypted_collection(&db, "case_2", master_key.clone())
            .await
            .1;
        assert!(
            result.as_ref().unwrap_err().is_invalid_argument(),
            "Expected invalid argument error, got {result:?}"
        );

        // Case 3: Invalid keyId
        let result = ce
            .create_encrypted_collection(&db, "case_1", master_key.clone())
            .encrypted_fields(doc! {
                "fields": [{
                    "path": "ssn",
                    "bsonType": "string",
                    "keyId": false,
                }],
            })
            .await
            .1;
        assert!(
            result.as_ref().unwrap_err().code() == Some(14),
            "Expected error 14 (type mismatch), got {result:?}"
        );

        // Case 4: Insert encrypted value
        let (ef, result) = ce
            .create_encrypted_collection(&db, "case_4", master_key.clone())
            .encrypted_fields(doc! {
                "fields": [{
                    "path": "ssn",
                    "bsonType": "string",
                    "keyId": Bson::Null,
                }],
            })
            .await;
        result?;
        let key = match ef.get_array("fields")?[0]
            .as_document()
            .unwrap()
            .get("keyId")
            .unwrap()
        {
            Bson::Binary(bin) => bin.clone(),
            v => panic!("invalid keyId {v:?}"),
        };
        let encrypted_payload = ce.encrypt("123-45-6789", key, Algorithm::Unindexed).await?;
        let coll = db.collection::<Document>("case_1");
        coll.insert_one(doc! { "ssn": encrypted_payload }).await?;

        Ok(())
    }

    #[tokio::test]
    async fn local() -> Result<()> {
        auto_encryption_keys(LocalMasterKey::builder().build()).await
    }

    #[tokio::test]
    async fn aws() -> Result<()> {
        auto_encryption_keys(
            AwsMasterKey::builder()
                .region("us-east-1")
                .key("arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0")
                .build(),
        )
        .await
    }
}

// Prose test 22. Range Explicit Encryption
mod range_explicit_encryption {
    use super::*;

    async fn range_explicit_encryption_test(
        bson_type: &str,
        range_options: RangeOptions,
    ) -> Result<()> {
        let util_client = Client::for_test().await;

        let encrypted_fields =
            load_testdata(&format!("data/range-encryptedFields-{bson_type}.json"))?;

        let key1_document = load_testdata("data/keys/key1-document.json")?;
        let key1_id = match key1_document.get("_id").unwrap() {
            Bson::Binary(binary) => binary,
            _ => unreachable!(),
        }
        .clone();

        let explicit_encryption_collection = util_client
            .database("db")
            .collection::<Document>("explicit_encryption");
        explicit_encryption_collection
            .drop()
            .encrypted_fields(encrypted_fields.clone())
            .await?;
        util_client
            .database("db")
            .create_collection("explicit_encryption")
            .encrypted_fields(encrypted_fields.clone())
            .await?;

        let datakeys_collection = util_client
            .database("keyvault")
            .collection::<Document>("datakeys");
        datakeys_collection.drop().await?;
        util_client
            .database("keyvault")
            .create_collection("datakeys")
            .await?;

        datakeys_collection
            .insert_one(key1_document)
            .write_concern(WriteConcern::majority())
            .await?;

        let key_vault_client = Client::for_test().await;

        let client_encryption = ClientEncryption::new(
            key_vault_client.into_client(),
            KV_NAMESPACE.clone(),
            vec![LOCAL_KMS.clone()],
        )?;

        let encrypted_client = Client::encrypted_builder(
            get_client_options().await.clone(),
            KV_NAMESPACE.clone(),
            vec![LOCAL_KMS.clone()],
        )?
        .extra_options(EXTRA_OPTIONS.clone())
        .bypass_query_analysis(true)
        .build()
        .await?;

        let key = format!("encrypted{bson_type}");
        let bson_numbers: BTreeMap<i32, RawBson> = [0, 6, 30, 200]
            .iter()
            .map(|num| (*num, get_raw_bson_from_num(bson_type, *num)))
            .collect();
        let explicit_encryption_collection = encrypted_client
            .database("db")
            .collection("explicit_encryption");

        for (id, num) in bson_numbers.keys().enumerate() {
            let encrypted_value = client_encryption
                .encrypt(bson_numbers[num].clone(), key1_id.clone(), Algorithm::Range)
                .contention_factor(0)
                .range_options(range_options.clone())
                .await?;

            explicit_encryption_collection
                .insert_one(doc! {
                    &key: encrypted_value,
                    "_id": id as i32,
                })
                .await?;
        }

        // Case 1: Decrypt a payload
        let insert_payload = client_encryption
            .encrypt(bson_numbers[&6].clone(), key1_id.clone(), Algorithm::Range)
            .contention_factor(0)
            .range_options(range_options.clone())
            .await?;

        let decrypted = client_encryption
            .decrypt(insert_payload.as_raw_binary())
            .await?;
        assert_eq!(decrypted, bson_numbers[&6]);

        // Utilities for cases 2-5
        let explicit_encryption_collection =
            explicit_encryption_collection.clone_with_type::<RawDocumentBuf>();
        let find_options = FindOptions::builder().sort(doc! { "_id": 1 }).build();
        let assert_success = |actual: Vec<RawDocumentBuf>, expected: &[i32]| {
            assert_eq!(actual.len(), expected.len());
            for (idx, num) in expected.iter().enumerate() {
                assert_eq!(
                    actual[idx].get(&key).unwrap(),
                    Some(bson_numbers[num].as_raw_bson_ref())
                );
            }
        };

        // Case 2: Find encrypted range and return the maximum
        let ckey: &crate::bson_compat::CStr = key.as_str().try_into()?;
        let query = rawdoc! {
            "$and": [
                { ckey: { "$gte": bson_numbers[&6].clone() } },
                { ckey: { "$lte": bson_numbers[&200].clone() } },
            ]
        };
        let find_payload = client_encryption
            .encrypt_expression(query, key1_id.clone())
            .contention_factor(0)
            .range_options(range_options.clone())
            .await?;

        let docs: Vec<RawDocumentBuf> = explicit_encryption_collection
            .find(find_payload)
            .with_options(find_options.clone())
            .await?
            .try_collect()
            .await?;
        assert_success(docs, &[6, 30, 200]);

        // Case 3: Find encrypted range and return the minimum
        let query = rawdoc! {
            "$and": [
                { ckey: { "$gte": bson_numbers[&0].clone() } },
                { ckey: { "$lte": bson_numbers[&6].clone() } },
            ]
        };
        let find_payload = client_encryption
            .encrypt_expression(query, key1_id.clone())
            .contention_factor(0)
            .range_options(range_options.clone())
            .await?;

        let docs: Vec<RawDocumentBuf> = encrypted_client
            .database("db")
            .collection("explicit_encryption")
            .find(find_payload)
            .with_options(find_options.clone())
            .await?
            .try_collect()
            .await?;
        assert_success(docs, &[0, 6]);

        // Case 4: Find encrypted range with an open range query
        let query = rawdoc! {
            "$and": [
                { ckey: { "$gt": bson_numbers[&30].clone() } },
            ]
        };
        let find_payload = client_encryption
            .encrypt_expression(query, key1_id.clone())
            .contention_factor(0)
            .range_options(range_options.clone())
            .await?;

        let docs: Vec<RawDocumentBuf> = encrypted_client
            .database("db")
            .collection("explicit_encryption")
            .find(find_payload)
            .with_options(find_options.clone())
            .await?
            .try_collect()
            .await?;
        assert_success(docs, &[200]);

        // Case 5: Run an aggregation expression inside $expr
        let query = rawdoc! { "$and": [ { "$lt": [ format!("${key}"), get_raw_bson_from_num(bson_type, 30) ] } ] };
        let find_payload = client_encryption
            .encrypt_expression(query, key1_id.clone())
            .contention_factor(0)
            .range_options(range_options.clone())
            .await?;

        let docs: Vec<RawDocumentBuf> = encrypted_client
            .database("db")
            .collection("explicit_encryption")
            .find(doc! { "$expr": find_payload })
            .with_options(find_options.clone())
            .await?
            .try_collect()
            .await?;
        assert_success(docs, &[0, 6]);

        // Case 6: Encrypting a document greater than the maximum errors
        if bson_type != "DoubleNoPrecision" && bson_type != "DecimalNoPrecision" {
            let num = get_raw_bson_from_num(bson_type, 201);
            let error = client_encryption
                .encrypt(num, key1_id.clone(), Algorithm::Range)
                .contention_factor(0)
                .range_options(range_options.clone())
                .await
                .unwrap_err();
            assert!(matches!(*error.kind, ErrorKind::Encryption(_)));
        }

        // Case 7: Encrypting a document of a different type errors
        if bson_type != "DoubleNoPrecision" && bson_type != "DecimalNoPrecision" {
            let value = if bson_type == "Int" {
                rawdoc! { ckey: { "$numberDouble": "6" } }
            } else {
                rawdoc! { ckey: { "$numberInt": "6" } }
            };
            let error = client_encryption
                .encrypt(value, key1_id.clone(), Algorithm::Range)
                .contention_factor(0)
                .range_options(range_options.clone())
                .await
                .unwrap_err();
            assert!(matches!(*error.kind, ErrorKind::Encryption(_)));
        }

        // Case 8: Setting precision errors if the type is not a double
        if !bson_type.contains("Double") && !bson_type.contains("Decimal") {
            let range_options = RangeOptions::builder()
                .sparsity(1)
                .min(get_bson_from_num(bson_type, 0))
                .max(get_bson_from_num(bson_type, 200))
                .precision(2)
                .build();
            let error = client_encryption
                .encrypt(bson_numbers[&6].clone(), key1_id.clone(), Algorithm::Range)
                .contention_factor(0)
                .range_options(range_options)
                .await
                .unwrap_err();
            assert!(matches!(*error.kind, ErrorKind::Encryption(_)));
        }

        Ok(())
    }

    fn get_bson_from_num(bson_type: &str, num: i32) -> Bson {
        match bson_type {
            "DecimalNoPrecision" | "DecimalPrecision" => {
                Bson::Decimal128(num.to_string().parse().unwrap())
            }
            "DoubleNoPrecision" | "DoublePrecision" => Bson::Double(num as f64),
            "Date" => Bson::DateTime(DateTime::from_millis(num as i64)),
            "Int" => Bson::Int32(num),
            "Long" => Bson::Int64(num as i64),
            _ => unreachable!(),
        }
    }

    fn get_raw_bson_from_num(bson_type: &str, num: i32) -> RawBson {
        match bson_type {
            "DecimalNoPrecision" | "DecimalPrecision" => {
                RawBson::Decimal128(num.to_string().parse().unwrap())
            }
            "DoubleNoPrecision" | "DoublePrecision" => RawBson::Double(num as f64),
            "Date" => RawBson::DateTime(DateTime::from_millis(num as i64)),
            "Int" => RawBson::Int32(num),
            "Long" => RawBson::Int64(num as i64),
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn range_explicit_encryption() -> Result<()> {
        if server_version_lt(8, 0).await || topology_is_standalone().await {
            log_uncaptured("Skipping range_explicit_encryption due to unsupported topology");
            return Ok(());
        }

        range_explicit_encryption_test(
            "DecimalNoPrecision",
            RangeOptions::builder().sparsity(1).trim_factor(1).build(),
        )
        .await?;
        range_explicit_encryption_test(
            "DecimalPrecision",
            RangeOptions::builder()
                .trim_factor(1)
                .sparsity(1)
                .min(Bson::Decimal128("0".parse()?))
                .max(Bson::Decimal128("200".parse()?))
                .precision(2)
                .build(),
        )
        .await?;
        range_explicit_encryption_test(
            "DoubleNoPrecision",
            RangeOptions::builder().trim_factor(1).sparsity(1).build(),
        )
        .await?;
        range_explicit_encryption_test(
            "DoublePrecision",
            RangeOptions::builder()
                .trim_factor(1)
                .sparsity(1)
                .min(Bson::Double(0.0))
                .max(Bson::Double(200.0))
                .precision(2)
                .build(),
        )
        .await?;
        range_explicit_encryption_test(
            "Date",
            RangeOptions::builder()
                .trim_factor(1)
                .sparsity(1)
                .min(Bson::DateTime(DateTime::from_millis(0)))
                .max(Bson::DateTime(DateTime::from_millis(200)))
                .build(),
        )
        .await?;
        range_explicit_encryption_test(
            "Int",
            RangeOptions::builder()
                .trim_factor(1)
                .sparsity(1)
                .min(Bson::Int32(0))
                .max(Bson::Int32(200))
                .build(),
        )
        .await?;
        range_explicit_encryption_test(
            "Long",
            RangeOptions::builder()
                .trim_factor(1)
                .sparsity(1)
                .min(Bson::Int64(0))
                .max(Bson::Int64(200))
                .build(),
        )
        .await?;

        Ok(())
    }
}

// Prose test 23. Range explicit encryption applies defaults
#[tokio::test]
async fn range_explicit_encryption_defaults() -> Result<()> {
    // Setup
    let key_vault_client = Client::for_test().await;
    let client_encryption = ClientEncryption::new(
        key_vault_client.into_client(),
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )?;
    let key_id = client_encryption
        .create_data_key(LocalMasterKey::builder().build())
        .await?;
    let payload_defaults = client_encryption
        .encrypt(123, key_id.clone(), Algorithm::Range)
        .contention_factor(0)
        .range_options(
            RangeOptions::builder()
                .min(Bson::from(0))
                .max(Bson::from(1000))
                .build(),
        )
        .await?;

    // Case 1: Uses libmongocrypt defaults
    let payload = client_encryption
        .encrypt(123, key_id.clone(), Algorithm::Range)
        .contention_factor(0)
        .range_options(
            RangeOptions::builder()
                .min(Bson::from(0))
                .max(Bson::from(1000))
                .sparsity(2)
                .trim_factor(6)
                .build(),
        )
        .await?;
    assert_eq!(payload_defaults.bytes.len(), payload.bytes.len());

    // Case 2: Accepts trimFactor 0
    let payload = client_encryption
        .encrypt(123, key_id.clone(), Algorithm::Range)
        .contention_factor(0)
        .range_options(
            RangeOptions::builder()
                .min(Bson::from(0))
                .max(Bson::from(1000))
                .trim_factor(0)
                .build(),
        )
        .await?;
    assert!(payload.bytes.len() > payload_defaults.bytes.len());

    Ok(())
}

// FLE 2.0 Documentation Example
#[tokio::test]
async fn fle2_example() -> Result<()> {
    if !fle2v2_ok("fle2_example").await {
        return Ok(());
    }

    let test_client = Client::for_test().await;

    // Drop data from prior test runs.
    test_client
        .database("keyvault")
        .collection::<Document>("datakeys")
        .drop()
        .await?;
    test_client.database("docsExamples").drop().await?;

    // Create two data keys.
    let ce = ClientEncryption::new(
        test_client.clone().into_client(),
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )?;
    let key1_id = ce
        .create_data_key(LocalMasterKey::builder().build())
        .await?;
    let key2_id = ce
        .create_data_key(LocalMasterKey::builder().build())
        .await?;

    // Create an encryptedFieldsMap.
    let encrypted_fields_map = [(
        "docsExamples.encrypted",
        doc! {
            "fields": [
                {
                    "path":     "encryptedIndexed",
                    "bsonType": "string",
                    "keyId":    key1_id,
                    "queries": { "queryType": "equality" },
                },
                {
                    "path":     "encryptedUnindexed",
                    "bsonType": "string",
                    "keyId":    key2_id,
                },
            ]
        },
    )];

    // Create an FLE 2 collection.
    let encrypted_client = Client::encrypted_builder(
        get_client_options().await.clone(),
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )?
    .extra_options(EXTRA_OPTIONS.clone())
    .encrypted_fields_map(encrypted_fields_map)
    .build()
    .await?;
    let db = encrypted_client.database("docsExamples");
    db.create_collection("encrypted").await?;
    let encrypted_coll = db.collection::<Document>("encrypted");

    // Auto encrypt an insert and find.

    // Encrypt an insert.
    encrypted_coll
        .insert_one(doc! {
            "_id":                1,
            "encryptedIndexed":   "indexedValue",
            "encryptedUnindexed": "unindexedValue",
        })
        .await?;

    // Encrypt a find.
    let found = encrypted_coll
        .find_one(doc! {
            "encryptedIndexed": "indexedValue",
        })
        .await?
        .unwrap();
    assert_eq!("indexedValue", found.get_str("encryptedIndexed")?);
    assert_eq!("unindexedValue", found.get_str("encryptedUnindexed")?);

    // Find documents without decryption.
    let unencrypted_coll = test_client
        .database("docsExamples")
        .collection::<Document>("encrypted");
    let found = unencrypted_coll.find_one(doc! { "_id": 1 }).await?.unwrap();
    assert_eq!(
        Some(ElementType::Binary),
        found.get("encryptedIndexed").map(Bson::element_type)
    );
    assert_eq!(
        Some(ElementType::Binary),
        found.get("encryptedUnindexed").map(Bson::element_type)
    );

    Ok(())
}

#[tokio::test]
async fn encrypt_expression_with_options() {
    let key_vault_client = Client::for_test().await.into_client();
    let client_encryption = ClientEncryption::new(
        key_vault_client,
        KV_NAMESPACE.clone(),
        vec![LOCAL_KMS.clone()],
    )
    .unwrap();
    let data_key = client_encryption
        .create_data_key(LocalMasterKey::builder().build())
        .await
        .unwrap();

    let expression = rawdoc! {
        "$and": [
            { "a": { "$gt": 0 } },
            { "a": { "$lt": 10 } },
        ]
    };
    let range_options = RangeOptions::builder()
        .min(Bson::from(0))
        .max(Bson::from(10))
        .build();

    let invalid_encrypt_options = EncryptOptions::builder()
        .contention_factor(0)
        .range_options(range_options.clone())
        .query_type("bad".to_string())
        .build();
    let error = client_encryption
        .encrypt_expression(expression.clone(), data_key.clone())
        .with_options(invalid_encrypt_options)
        .await
        .unwrap_err();
    assert!(matches!(*error.kind, ErrorKind::InvalidArgument { .. }));

    let valid_encrypt_options = EncryptOptions::builder()
        .contention_factor(0)
        .range_options(range_options)
        .build();
    client_encryption
        .encrypt_expression(expression, data_key)
        .with_options(valid_encrypt_options)
        .await
        .unwrap();
}
