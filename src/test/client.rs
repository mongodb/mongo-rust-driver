use std::{borrow::Cow, collections::HashMap, future::IntoFuture, time::Duration};

use bson::Document;
use serde::{Deserialize, Serialize};

use crate::{
    bson::{doc, Bson},
    error::{CommandError, Error, ErrorKind},
    event::{cmap::CmapEvent, sdam::SdamEvent},
    hello::LEGACY_HELLO_COMMAND_NAME,
    options::{AuthMechanism, ClientOptions, Credential, ServerAddress},
    runtime,
    selection_criteria::{ReadPreference, ReadPreferenceOptions, SelectionCriteria},
    test::{
        get_client_options,
        log_uncaptured,
        util::{
            event_buffer::{EventBuffer, EventStream},
            fail_point::{FailPoint, FailPointMode},
            TestClient,
        },
        Event,
        SERVER_API,
    },
    Client,
    ServerType,
};

#[derive(Debug, Deserialize)]
struct ClientMetadata {
    pub driver: DriverMetadata,
    #[serde(rename = "os")]
    pub _os: Document, // included here to ensure it's included in the metadata
    pub platform: String,
}

#[derive(Debug, Deserialize)]
struct DriverMetadata {
    pub name: String,
    pub version: String,
}

#[tokio::test]
async fn metadata_sent_in_handshake() {
    let client = Client::for_test().await;

    // skip on other topologies due to different currentOp behavior
    if !client.is_standalone() || !client.is_replica_set() {
        log_uncaptured("skipping metadata_sent_in_handshake due to unsupported topology");
        return;
    }

    let result = client
        .database("admin")
        .run_command(doc! {
            "currentOp": 1,
            "command.currentOp": { "$exists": true }
        })
        .await
        .unwrap();

    let metadata_document = result.get_array("inprog").unwrap()[0]
        .as_document()
        .unwrap()
        .get_document("clientMetadata")
        .unwrap()
        .clone();
    let metadata: ClientMetadata = bson::from_document(metadata_document).unwrap();

    assert_eq!(metadata.driver.name, "mongo-rust-driver");
    assert_eq!(metadata.driver.version, env!("CARGO_PKG_VERSION"));

    assert!(
        metadata.platform.contains("tokio"),
        "platform should contain tokio: {}",
        metadata.platform
    );

    #[cfg(feature = "sync")]
    {
        assert!(
            metadata.platform.contains("sync"),
            "platform should contain sync: {}",
            metadata.platform
        );
    }
}

#[tokio::test]
#[function_name::named]
async fn connection_drop_during_read() {
    let mut options = get_client_options().await.clone();
    options.max_pool_size = Some(1);

    let client = Client::with_options(options.clone()).unwrap();
    let db = client.database("test");

    db.collection(function_name!())
        .insert_one(doc! { "x": 1 })
        .await
        .unwrap();

    let _: Result<_, _> = runtime::timeout(
        Duration::from_millis(50),
        db.run_command(doc! {
            "count": function_name!(),
            "query": {
                "$where": "sleep(100) && true"
            }
        })
        .into_future(),
    )
    .await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    let build_info_response = db.run_command(doc! { "buildInfo": 1 }).await.unwrap();

    // Ensure that the response to `buildInfo` is read, not the response to `count`.
    assert!(build_info_response.get("version").is_some());
}

#[tokio::test]
async fn server_selection_timeout_message() {
    if get_client_options().await.repl_set_name.is_none() {
        log_uncaptured("skipping server_selection_timeout_message due to missing replica set name");
        return;
    }

    let mut tag_set = HashMap::new();
    tag_set.insert("asdfasdf".to_string(), "asdfadsf".to_string());

    let unsatisfiable_read_preference = ReadPreference::Secondary {
        options: Some(
            ReadPreferenceOptions::builder()
                .tag_sets(vec![tag_set])
                .build(),
        ),
    };

    let mut options = get_client_options().await.clone();
    options.server_selection_timeout = Some(Duration::from_millis(500));

    let client = Client::with_options(options.clone()).unwrap();
    let db = client.database("test");
    let error = db
        .run_command(doc! { "ping": 1 })
        .selection_criteria(SelectionCriteria::ReadPreference(
            unsatisfiable_read_preference,
        ))
        .await
        .expect_err("should fail with server selection timeout error");

    let error_description = format!("{}", error);
    for host in options.hosts.iter() {
        assert!(error_description.contains(format!("{}", host).as_str()));
    }
}

#[tokio::test]
#[function_name::named]
async fn list_databases() {
    let expected_dbs = &[
        format!("{}1", function_name!()),
        format!("{}2", function_name!()),
        format!("{}3", function_name!()),
    ];

    let client = Client::for_test().await;

    for name in expected_dbs {
        client.database(name).drop().await.unwrap();
    }

    let prev_dbs = client.list_databases().await.unwrap();

    for name in expected_dbs {
        assert!(!prev_dbs.iter().any(|doc| doc.name.as_str() == name));

        let db = client.database(name);

        db.collection("foo")
            .insert_one(doc! { "x": 1 })
            .await
            .unwrap();
    }

    let new_dbs = client.list_databases().await.unwrap();
    let new_dbs: Vec<_> = new_dbs
        .into_iter()
        .filter(|db_spec| expected_dbs.contains(&db_spec.name))
        .collect();
    assert_eq!(new_dbs.len(), expected_dbs.len());

    for name in expected_dbs {
        let db_doc = new_dbs
            .iter()
            .find(|db_spec| db_spec.name.as_str() == name)
            .unwrap();
        assert!(db_doc.size_on_disk > 0);
        assert!(!db_doc.empty);
    }
}

#[tokio::test]
#[function_name::named]
async fn list_database_names() {
    let client = Client::for_test().await;

    let expected_dbs = &[
        format!("{}1", function_name!()),
        format!("{}2", function_name!()),
        format!("{}3", function_name!()),
    ];

    for name in expected_dbs {
        client.database(name).drop().await.unwrap();
    }

    let prev_dbs = client.list_database_names().await.unwrap();

    for name in expected_dbs {
        assert!(!prev_dbs.iter().any(|db_name| db_name == name));

        let db = client.database(name);

        db.collection("foo")
            .insert_one(doc! { "x": 1 })
            .await
            .unwrap();
    }

    let new_dbs = client.list_database_names().await.unwrap();

    for name in expected_dbs {
        assert_eq!(new_dbs.iter().filter(|db_name| db_name == &name).count(), 1);
    }
}

#[tokio::test]
#[function_name::named]
async fn list_authorized_databases() {
    let client = Client::for_test().await;
    if client.server_version_lt(4, 0) || !client.auth_enabled() {
        log_uncaptured("skipping list_authorized_databases due to test configuration");
        return;
    }

    let dbs = &[
        format!("{}1", function_name!()),
        format!("{}2", function_name!()),
    ];

    for name in dbs {
        client
            .database(name)
            .create_collection("coll")
            .await
            .unwrap();
        client
            .create_user(
                &format!("user_{}", name),
                "pwd",
                &[Bson::from(doc! { "role": "readWrite", "db": name })],
                &[AuthMechanism::ScramSha256],
                None,
            )
            .await
            .unwrap();
    }

    for name in dbs {
        let mut options = get_client_options().await.clone();
        let credential = Credential::builder()
            .username(format!("user_{}", name))
            .password(String::from("pwd"))
            .build();
        options.credential = Some(credential);
        let client = Client::with_options(options).unwrap();

        let result = client
            .list_database_names()
            .authorized_databases(true)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result.first().unwrap(), name);
    }

    for name in dbs {
        client.database(name).drop().await.unwrap();
    }
}

fn is_auth_error(error: Error) -> bool {
    matches!(*error.kind, ErrorKind::Authentication { .. })
}

/// Performs an operation that requires authentication and verifies that it either succeeded or
/// failed with an authentication error according to the `should_succeed` parameter.
async fn auth_test(client: Client, should_succeed: bool) {
    let result = client.list_database_names().await;
    if should_succeed {
        result.expect("operation should have succeeded");
    } else {
        assert!(is_auth_error(result.unwrap_err()));
    }
}

/// Attempts to authenticate using the given username/password, optionally specifying a mechanism
/// via the `ClientOptions` api.
///
/// Asserts that the authentication's success matches the provided parameter.
async fn auth_test_options(
    user: &str,
    password: &str,
    mechanism: Option<AuthMechanism>,
    success: bool,
) {
    let mut options = get_client_options().await.clone();
    options.max_pool_size = Some(1);
    options.credential = Credential {
        username: Some(user.to_string()),
        password: Some(password.to_string()),
        mechanism,
        ..Default::default()
    }
    .into();

    auth_test(Client::with_options(options).unwrap(), success).await;
}

/// Attempts to authenticate using the given username/password, optionally specifying a mechanism
/// via the URI api.
///
/// Asserts that the authentication's success matches the provided parameter.
async fn auth_test_uri(
    user: &str,
    password: &str,
    mechanism: Option<AuthMechanism>,
    should_succeed: bool,
) {
    // A server API version cannot be set in the connection string.
    if SERVER_API.is_some() {
        log_uncaptured("Skipping URI auth test due to server API version being set");
        return;
    }

    let host = get_client_options()
        .await
        .hosts
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<String>>()
        .join(",");
    let mechanism_str = match mechanism {
        Some(mech) => Cow::Owned(format!("&authMechanism={}", mech.as_str())),
        None => Cow::Borrowed(""),
    };
    let mut uri = format!(
        "mongodb://{}:{}@{}/?maxPoolSize=1{}",
        user,
        password,
        host,
        mechanism_str.as_ref()
    );

    if let Some(ref tls_options) = get_client_options().await.tls_options() {
        if let Some(true) = tls_options.allow_invalid_certificates {
            uri.push_str("&tlsAllowInvalidCertificates=true");
        }

        if let Some(ref ca_file_path) = tls_options.ca_file_path {
            uri.push_str("&tlsCAFile=");
            uri.push_str(
                &percent_encoding::utf8_percent_encode(
                    ca_file_path.to_str().unwrap(),
                    percent_encoding::NON_ALPHANUMERIC,
                )
                .to_string(),
            );
        }

        if let Some(ref cert_key_file_path) = tls_options.cert_key_file_path {
            uri.push_str("&tlsCertificateKeyFile=");
            uri.push_str(
                &percent_encoding::utf8_percent_encode(
                    cert_key_file_path.to_str().unwrap(),
                    percent_encoding::NON_ALPHANUMERIC,
                )
                .to_string(),
            );
        }
    }

    if let Some(true) = get_client_options().await.load_balanced {
        uri.push_str("&loadBalanced=true");
    }

    auth_test(
        Client::with_uri_str(uri.as_str()).await.unwrap(),
        should_succeed,
    )
    .await;
}

/// Tries to authenticate with the given credentials using the given mechanisms, both by explicitly
/// specifying each mechanism and by relying on mechanism negotiation.
///
/// If only one mechanism is supplied, this will also test that using the other SCRAM mechanism will
/// fail.
async fn scram_test(
    client: &TestClient,
    username: &str,
    password: &str,
    mechanisms: &[AuthMechanism],
) {
    for mechanism in mechanisms {
        auth_test_uri(username, password, Some(mechanism.clone()), true).await;
        auth_test_uri(username, password, None, true).await;
        auth_test_options(username, password, Some(mechanism.clone()), true).await;
        auth_test_options(username, password, None, true).await;
    }

    // If only one scram mechanism is specified, verify the other doesn't work.
    if mechanisms.len() == 1 && client.server_version_gte(4, 0) {
        let other = match mechanisms[0] {
            AuthMechanism::ScramSha1 => AuthMechanism::ScramSha256,
            _ => AuthMechanism::ScramSha1,
        };
        auth_test_uri(username, password, Some(other.clone()), false).await;
        auth_test_options(username, password, Some(other), false).await;
    }
}

#[tokio::test]
async fn scram_sha1() {
    let client = Client::for_test().await;
    if !client.auth_enabled() {
        log_uncaptured("skipping scram_sha1 due to missing authentication");
        return;
    }

    client
        .create_user(
            "sha1",
            "sha1",
            &[Bson::from("root")],
            &[AuthMechanism::ScramSha1],
            None,
        )
        .await
        .unwrap();
    scram_test(&client, "sha1", "sha1", &[AuthMechanism::ScramSha1]).await;
}

#[tokio::test]
async fn scram_sha256() {
    let client = Client::for_test().await;
    if client.server_version_lt(4, 0) || !client.auth_enabled() {
        log_uncaptured("skipping scram_sha256 due to test configuration");
        return;
    }
    client
        .create_user(
            "sha256",
            "sha256",
            &[Bson::from("root")],
            &[AuthMechanism::ScramSha256],
            None,
        )
        .await
        .unwrap();
    scram_test(&client, "sha256", "sha256", &[AuthMechanism::ScramSha256]).await;
}

#[tokio::test]
async fn scram_both() {
    let client = Client::for_test().await;
    if client.server_version_lt(4, 0) || !client.auth_enabled() {
        log_uncaptured("skipping scram_both due to test configuration");
        return;
    }
    client
        .create_user(
            "both",
            "both",
            &[Bson::from("root")],
            &[AuthMechanism::ScramSha1, AuthMechanism::ScramSha256],
            None,
        )
        .await
        .unwrap();
    scram_test(
        &client,
        "both",
        "both",
        &[AuthMechanism::ScramSha1, AuthMechanism::ScramSha256],
    )
    .await;
}

#[tokio::test]
async fn scram_missing_user_uri() {
    let client = Client::for_test().await;
    if !client.auth_enabled() {
        log_uncaptured("skipping scram_missing_user_uri due to missing authentication");
        return;
    }
    auth_test_uri("adsfasdf", "ASsdfsadf", None, false).await;
}

#[tokio::test]
async fn scram_missing_user_options() {
    let client = Client::for_test().await;
    if !client.auth_enabled() {
        log_uncaptured("skipping scram_missing_user_options due to missing authentication");
        return;
    }
    auth_test_options("sadfasdf", "fsdadsfasdf", None, false).await;
}

#[tokio::test]
async fn saslprep() {
    let client = Client::for_test().await;

    if client.server_version_lt(4, 0) || !client.auth_enabled() {
        log_uncaptured("skipping saslprep due to test configuration");
        return;
    }

    client
        .create_user(
            "IX",
            "IX",
            &[Bson::from("root")],
            &[AuthMechanism::ScramSha256],
            None,
        )
        .await
        .unwrap();
    client
        .create_user(
            "\u{2168}",
            "\u{2163}",
            &[Bson::from("root")],
            &[AuthMechanism::ScramSha256],
            None,
        )
        .await
        .unwrap();

    auth_test_options("IX", "IX", None, true).await;
    auth_test_options("IX", "I\u{00AD}X", None, true).await;
    auth_test_options("\u{2168}", "IV", None, true).await;
    auth_test_options("\u{2168}", "I\u{00AD}V", None, true).await;

    auth_test_uri("IX", "IX", None, true).await;
    auth_test_uri("IX", "I%C2%ADX", None, true).await;
    auth_test_uri("%E2%85%A8", "IV", None, true).await;
    auth_test_uri("%E2%85%A8", "I%C2%ADV", None, true).await;
}

#[tokio::test]
#[function_name::named]
async fn x509_auth() {
    let username = match std::env::var("MONGO_X509_USER") {
        Ok(user) => user,
        Err(_) => return,
    };

    let client = Client::for_test().await;
    let drop_user_result = client
        .database("$external")
        .run_command(doc! { "dropUser": &username })
        .await;

    match drop_user_result.map_err(|e| *e.kind) {
        Err(ErrorKind::Command(CommandError { code: 11, .. })) | Ok(_) => {}
        e @ Err(_) => {
            e.unwrap();
        }
    };

    client
        .create_user(
            &username,
            None,
            &[doc! { "role": "readWrite", "db": function_name!() }.into()],
            &[AuthMechanism::MongoDbX509],
            "$external",
        )
        .await
        .unwrap();

    let mut options = get_client_options().await.clone();
    options.credential = Some(
        Credential::builder()
            .mechanism(AuthMechanism::MongoDbX509)
            .build(),
    );

    let client = Client::for_test().options(options).await;
    client
        .database(function_name!())
        .collection::<Document>(function_name!())
        .find_one(doc! {})
        .await
        .unwrap();
}

#[tokio::test]
async fn plain_auth() {
    if std::env::var("MONGO_PLAIN_AUTH_TEST").is_err() {
        log_uncaptured("skipping plain_auth due to environment variable MONGO_PLAIN_AUTH_TEST");
        return;
    }

    let options = ClientOptions::builder()
        .hosts(vec![ServerAddress::Tcp {
            host: "ldaptest.10gen.cc".into(),
            port: None,
        }])
        .credential(
            Credential::builder()
                .mechanism(AuthMechanism::Plain)
                .username("drivers-team".to_string())
                .password("mongor0x$xgen".to_string())
                .build(),
        )
        .build();

    let client = Client::with_options(options).unwrap();
    let coll = client.database("ldap").collection("test");

    let doc = coll.find_one(doc! {}).await.unwrap().unwrap();

    #[derive(Debug, Deserialize, PartialEq)]
    struct TestDocument {
        ldap: bool,
        authenticated: String,
    }

    let doc: TestDocument = bson::from_document(doc).unwrap();

    assert_eq!(
        doc,
        TestDocument {
            ldap: true,
            authenticated: "yeah".into()
        }
    );
}

/// Test verifies that retrying a commitTransaction operation after a checkOut
/// failure works.
#[tokio::test(flavor = "multi_thread")]
async fn retry_commit_txn_check_out() {
    let setup_client = Client::for_test().await;
    if !setup_client.is_replica_set() {
        log_uncaptured("skipping retry_commit_txn_check_out due to non-replicaset topology");
        return;
    }

    if !setup_client.supports_transactions() {
        log_uncaptured("skipping retry_commit_txn_check_out due to lack of transaction support");
        return;
    }

    if !setup_client.supports_fail_command_appname_initial_handshake() {
        log_uncaptured(
            "skipping retry_commit_txn_check_out due to insufficient failCommand support",
        );
        return;
    }

    if setup_client.supports_streaming_monitoring_protocol() {
        log_uncaptured("skipping retry_commit_txn_check_out due to streaming protocol support");
        return;
    }

    // ensure namespace exists
    setup_client
        .database("retry_commit_txn_check_out")
        .collection("retry_commit_txn_check_out")
        .insert_one(doc! {})
        .await
        .unwrap();

    let mut options = get_client_options().await.clone();
    let buffer = EventBuffer::new();
    options.cmap_event_handler = Some(buffer.handler());
    options.sdam_event_handler = Some(buffer.handler());
    options.heartbeat_freq = Some(Duration::from_secs(120));
    options.app_name = Some("retry_commit_txn_check_out".to_string());
    let client = Client::with_options(options).unwrap();

    let mut session = client.start_session().await.unwrap();
    session.start_transaction().await.unwrap();
    // transition transaction to "in progress" so that the commit
    // actually executes an operation.
    client
        .database("retry_commit_txn_check_out")
        .collection("retry_commit_txn_check_out")
        .insert_one(doc! {})
        .session(&mut session)
        .await
        .unwrap();

    // Enable a fail point that clears the connection pools so that commitTransaction will create a
    // new connection during checkout.
    let fail_point = FailPoint::fail_command(&["ping"], FailPointMode::Times(1)).error_code(11600);
    let _guard = setup_client.enable_fail_point(fail_point).await.unwrap();

    let mut event_stream = buffer.stream();
    client
        .database("foo")
        .run_command(doc! { "ping": 1 })
        .await
        .unwrap_err();

    // failing with a state change error will request an immediate check
    // wait for the mark unknown and subsequent succeeded heartbeat
    let mut primary = None;
    event_stream
        .next_match(Duration::from_secs(1), |e| {
            if let Event::Sdam(SdamEvent::ServerDescriptionChanged(event)) = e {
                if event.is_marked_unknown_event() {
                    primary = Some(event.address.clone());
                    return true;
                }
            }
            false
        })
        .await
        .expect("should see marked unknown event");

    // If this test were run when using the streaming protocol, this assertion would never succeed.
    // This is because the monitors are waiting for the next heartbeat from the server for
    // heartbeatFrequencyMS (which is 2 minutes) and ignore the immediate check requests from the
    // ping command in the meantime due to already being in the middle of their checks.
    event_stream
        .next_match(Duration::from_secs(1), |e| {
            if let Event::Sdam(SdamEvent::ServerDescriptionChanged(event)) = e {
                if &event.address == primary.as_ref().unwrap()
                    && event.previous_description.server_type() == ServerType::Unknown
                {
                    return true;
                }
            }
            false
        })
        .await
        .expect("should see mark available event");

    let fail_point = FailPoint::fail_command(
        &[LEGACY_HELLO_COMMAND_NAME, "hello"],
        FailPointMode::Times(1),
    )
    .error_code(11600)
    .app_name("retry_commit_txn_check_out");
    let _guard2 = setup_client.enable_fail_point(fail_point).await.unwrap();

    // finally, attempt the commit.
    // this should succeed due to retry
    session.commit_transaction().await.unwrap();

    // ensure the first check out attempt fails
    event_stream
        .next_match(Duration::from_secs(1), |e| {
            matches!(e, Event::Cmap(CmapEvent::ConnectionCheckoutFailed(_)))
        })
        .await
        .expect("should see check out failed event");

    // ensure the second one succeeds
    event_stream
        .next_match(Duration::from_secs(1), |e| {
            matches!(e, Event::Cmap(CmapEvent::ConnectionCheckedOut(_)))
        })
        .await
        .expect("should see checked out event");
}

/// Verifies that `Client::shutdown` succeeds.
#[tokio::test]
async fn manual_shutdown_with_nothing() {
    let client = Client::for_test().await.into_client();
    client.shutdown().await;
}

/// Verifies that `Client::shutdown` succeeds when resources have been dropped.
#[tokio::test]
async fn manual_shutdown_with_resources() {
    let client = Client::for_test().monitor_events().await;
    if !client.supports_transactions() {
        log_uncaptured("Skipping manual_shutdown_with_resources: no transaction support");
        return;
    }
    let db = client.database("shutdown_test");
    db.drop().await.unwrap();
    let coll = db.collection::<Document>("test");
    coll.insert_many([doc! {}, doc! {}]).await.unwrap();
    let bucket = db.gridfs_bucket(None);
    // Scope to force drop of resources
    {
        // Exhausted cursors don't need cleanup, so make sure there's more than one batch to fetch
        let _cursor = coll.find(doc! {}).batch_size(1).await.unwrap();
        // Similarly, sessions need an in-progress transaction to have cleanup.
        let mut session = client.start_session().await.unwrap();
        if session.start_transaction().await.is_err() {
            // Transaction start can transiently fail; if so, just bail out of the test.
            log_uncaptured("Skipping manual_shutdown_with_resources: transaction start failed");
            return;
        }
        if coll
            .insert_one(doc! {})
            .session(&mut session)
            .await
            .is_err()
        {
            // Likewise for transaction operations.
            log_uncaptured("Skipping manual_shutdown_with_resources: transaction operation failed");
            return;
        }
        let _stream = bucket.open_upload_stream("test").await.unwrap();
    }
    let is_sharded = client.is_sharded();
    let events = client.events.clone();
    client.into_client().shutdown().await;
    if !is_sharded {
        // killCursors doesn't always execute on sharded clusters due to connection pinning
        assert!(!events
            .get_command_started_events(&["killCursors"])
            .is_empty());
    }
    assert!(!events
        .get_command_started_events(&["abortTransaction"])
        .is_empty());
    assert!(!events.get_command_started_events(&["delete"]).is_empty());
}

/// Verifies that `Client::shutdown_immediate` succeeds.
#[tokio::test]
async fn manual_shutdown_immediate_with_nothing() {
    let client = Client::for_test().await.into_client();
    client.shutdown().immediate(true).await;
}

/// Verifies that `Client::shutdown_immediate` succeeds without waiting for resources.
#[tokio::test]
async fn manual_shutdown_immediate_with_resources() {
    let client = Client::for_test().monitor_events().await;
    if !client.supports_transactions() {
        log_uncaptured("Skipping manual_shutdown_immediate_with_resources: no transaction support");
        return;
    }
    let db = client.database("shutdown_test");
    db.drop().await.unwrap();
    let coll = db.collection::<Document>("test");
    coll.insert_many([doc! {}, doc! {}]).await.unwrap();
    let bucket = db.gridfs_bucket(None);

    // Resources are scoped to past the `shutdown_immediate`.

    // Exhausted cursors don't need cleanup, so make sure there's more than one batch to fetch
    let _cursor = coll.find(doc! {}).batch_size(1).await.unwrap();
    // Similarly, sessions need an in-progress transaction to have cleanup.
    let mut session = client.start_session().await.unwrap();
    session.start_transaction().await.unwrap();
    coll.insert_one(doc! {})
        .session(&mut session)
        .await
        .unwrap();
    let _stream = bucket.open_upload_stream("test").await.unwrap();

    let events = client.events.clone();
    client.into_client().shutdown().immediate(true).await;

    assert!(events
        .get_command_started_events(&["killCursors"])
        .is_empty());
    assert!(events
        .get_command_started_events(&["abortTransaction"])
        .is_empty());
    assert!(events.get_command_started_events(&["delete"]).is_empty());
}

#[tokio::test]
async fn find_one_and_delete_serde_consistency() {
    let client = Client::for_test().await;

    let coll = client
        .database("find_one_and_delete_serde_consistency")
        .collection("test");

    #[derive(Debug, Serialize, Deserialize)]
    struct Foo {
        #[serde(with = "serde_hex::SerHexSeq::<serde_hex::StrictPfx>")]
        problematic: Vec<u8>,
    }

    let doc = Foo {
        problematic: vec![0, 1, 2, 3, 4, 5, 6, 7],
    };

    coll.insert_one(&doc).await.unwrap();
    let rec: Foo = coll.find_one(doc! {}).await.unwrap().unwrap();
    assert_eq!(doc.problematic, rec.problematic);
    let rec: Foo = coll.find_one_and_delete(doc! {}).await.unwrap().unwrap();
    assert_eq!(doc.problematic, rec.problematic);

    let nothing = coll.find_one_and_delete(doc! {}).await.unwrap();
    assert!(nothing.is_none());
}

// Verifies that `Client::warm_connection_pool` succeeds.
#[tokio::test]
async fn warm_connection_pool() {
    let client = Client::for_test()
        .options({
            let mut opts = get_client_options().await.clone();
            opts.min_pool_size = Some(10);
            opts
        })
        .await;

    client.warm_connection_pool().await;
    // Validate that a command executes.
    client.list_database_names().await.unwrap();
}

async fn get_end_session_event_count<'a>(event_stream: &mut EventStream<'a, Event>) -> usize {
    // Use collect_successful_command_execution to assert that the call to endSessions succeeded.
    event_stream
        .collect_successful_command_execution(Duration::from_millis(500), "endSessions")
        .await
        .len()
}

#[tokio::test]
async fn end_sessions_on_drop() {
    let client1 = Client::for_test().monitor_events().await;
    let client2 = client1.clone();
    let events = client1.events.clone();
    let mut event_stream = events.stream();

    // Run an operation to populate the session pool.
    client1
        .database("db")
        .collection::<Document>("coll")
        .find(doc! {})
        .await
        .unwrap();

    drop(client1);
    assert_eq!(get_end_session_event_count(&mut event_stream).await, 0);

    drop(client2);
    assert_eq!(get_end_session_event_count(&mut event_stream).await, 1);
}

#[tokio::test]
async fn end_sessions_on_shutdown() {
    let client1 = Client::for_test().monitor_events().await;
    let client2 = client1.clone();
    let events = client1.events.clone();
    let mut event_stream = events.stream();

    // Run an operation to populate the session pool.
    client1
        .database("db")
        .collection::<Document>("coll")
        .find(doc! {})
        .await
        .unwrap();

    client1.into_client().shutdown().await;
    assert_eq!(get_end_session_event_count(&mut event_stream).await, 1);

    client2.into_client().shutdown().await;
    assert_eq!(get_end_session_event_count(&mut event_stream).await, 0);
}
