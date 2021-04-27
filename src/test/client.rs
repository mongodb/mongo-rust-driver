use std::{borrow::Cow, collections::HashMap, time::Duration};

use bson::Document;
use serde::Deserialize;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use crate::{
    bson::{doc, Bson},
    error::{CommandError, Error, ErrorKind},
    options::{AuthMechanism, ClientOptions, Credential, ListDatabasesOptions, StreamAddress},
    selection_criteria::{ReadPreference, ReadPreferenceOptions, SelectionCriteria},
    test::{util::TestClient, CLIENT_OPTIONS, LOCK},
    Client,
    RUNTIME,
};

#[derive(Debug, Deserialize)]
struct Metadata {
    #[serde(rename = "clientMetadata")]
    pub client: ClientMetadata,
}

#[derive(Debug, Deserialize)]
struct ClientMetadata {
    pub driver: DriverMetadata,
    pub os: OsMetadata,
}

#[derive(Debug, Deserialize)]
struct DriverMetadata {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Deserialize)]
struct OsMetadata {
    #[serde(rename = "type")]
    pub os_type: String,
    pub architecture: String,
}

// This test currently doesn't pass on replica sets and sharded clusters consistently due to
// `currentOp` sometimes detecting heartbeats between the server. Eventually we can test this using
// APM or coming up with something more clever, but for now, we're just disabling it.
//
// #[cfg_attr(feature = "tokio-runtime", tokio::test)]
// #[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[allow(unused)]
async fn metadata_sent_in_handshake() {
    let client = TestClient::new().await;
    let db = client.database("admin");
    let result = db.run_command(doc! { "currentOp": 1 }, None).await.unwrap();

    let in_prog = match result.get("inprog") {
        Some(Bson::Array(in_prog)) => in_prog,
        _ => panic!("no `inprog` array found in response to `currentOp`"),
    };

    let metadata: Metadata = bson::from_bson(in_prog[0].clone()).unwrap();
    assert_eq!(metadata.client.driver.name, "mrd");
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn connection_drop_during_read() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let mut options = CLIENT_OPTIONS.clone();
    options.max_pool_size = Some(1);

    let client = Client::with_options(options.clone()).unwrap();
    let db = client.database("test");

    db.collection(function_name!())
        .insert_one(doc! { "x": 1 }, None)
        .await
        .unwrap();

    let _: Result<_, _> = RUNTIME
        .timeout(
            Duration::from_millis(50),
            db.run_command(
                doc! {
                    "count": function_name!(),
                    "query": {
                        "$where": "sleep(100) && true"
                    }
                },
                None,
            ),
        )
        .await;

    RUNTIME.delay_for(Duration::from_millis(200)).await;

    let is_master_response = db.run_command(doc! { "isMaster": 1 }, None).await;

    // Ensure that the response to `isMaster` is read, not the response to `count`.
    assert!(is_master_response
        .ok()
        .and_then(|value| value.get("ismaster").and_then(|value| value.as_bool()))
        .is_some());
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn server_selection_timeout_message() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    if CLIENT_OPTIONS.repl_set_name.is_none() {
        return;
    }

    let mut tag_set = HashMap::new();
    tag_set.insert("asdfasdf".to_string(), "asdfadsf".to_string());

    let unsatisfiable_read_preference = ReadPreference::Secondary {
        options: ReadPreferenceOptions::builder()
            .tag_sets(vec![tag_set])
            .build(),
    };

    let mut options = CLIENT_OPTIONS.clone();
    options.server_selection_timeout = Some(Duration::from_millis(500));

    let client = Client::with_options(options.clone()).unwrap();
    let db = client.database("test");
    let error = db
        .run_command(
            doc! { "isMaster": 1 },
            SelectionCriteria::ReadPreference(unsatisfiable_read_preference),
        )
        .await
        .expect_err("should fail with server selection timeout error");

    let error_description = format!("{}", error);
    for host in options.hosts.iter() {
        assert!(error_description.contains(format!("{}", host).as_str()));
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn list_databases() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let expected_dbs = &[
        format!("{}1", function_name!()),
        format!("{}2", function_name!()),
        format!("{}3", function_name!()),
    ];

    let client = TestClient::new().await;

    for name in expected_dbs {
        client.database(name).drop(None).await.unwrap();
    }

    let prev_dbs = client.list_databases(None, None).await.unwrap();

    for name in expected_dbs {
        assert!(!prev_dbs.iter().any(|doc| doc.name.as_str() == name));

        let db = client.database(name);

        db.collection("foo")
            .insert_one(doc! { "x": 1 }, None)
            .await
            .unwrap();
    }

    let new_dbs = client.list_databases(None, None).await.unwrap();
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn list_database_names() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = TestClient::new().await;

    let expected_dbs = &[
        format!("{}1", function_name!()),
        format!("{}2", function_name!()),
        format!("{}3", function_name!()),
    ];

    for name in expected_dbs {
        client.database(name).drop(None).await.unwrap();
    }

    let prev_dbs = client.list_database_names(None, None).await.unwrap();

    for name in expected_dbs {
        assert!(!prev_dbs.iter().any(|db_name| db_name == name));

        let db = client.database(name);

        db.collection("foo")
            .insert_one(doc! { "x": 1 }, None)
            .await
            .unwrap();
    }

    let new_dbs = client.list_database_names(None, None).await.unwrap();

    for name in expected_dbs {
        assert_eq!(new_dbs.iter().filter(|db_name| db_name == &name).count(), 1);
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn list_authorized_databases() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = TestClient::new().await;
    if client.server_version_lt(4, 0) || !client.auth_enabled() {
        return;
    }

    let dbs = &[
        format!("{}1", function_name!()),
        format!("{}2", function_name!()),
    ];

    for name in dbs {
        client
            .database(name)
            .create_collection("coll", None)
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
        let mut options = CLIENT_OPTIONS.clone();
        let credential = Credential::builder()
            .username(format!("user_{}", name))
            .password(String::from("pwd"))
            .build();
        options.credential = Some(credential);
        let client = Client::with_options(options).unwrap();

        let options = ListDatabasesOptions::builder()
            .authorized_databases(true)
            .build();
        let result = client.list_database_names(None, options).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result.get(0).unwrap(), name);
    }

    for name in dbs {
        client.database(name).drop(None).await.unwrap();
    }
}

fn is_auth_error(error: Error) -> bool {
    matches!(error.kind, ErrorKind::AuthenticationError { .. })
}

/// Performs an operation that requires authentication and verifies that it either succeeded or
/// failed with an authentication error according to the `should_succeed` parameter.
async fn auth_test(client: Client, should_succeed: bool) {
    let result = client.list_database_names(None, None).await;
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
    let mut options = ClientOptions::builder()
        .hosts(CLIENT_OPTIONS.hosts.clone())
        .max_pool_size(1)
        .credential(Credential {
            username: Some(user.to_string()),
            password: Some(password.to_string()),
            mechanism,
            ..Default::default()
        })
        .build();
    options.tls = CLIENT_OPTIONS.tls.clone();

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
    let host = CLIENT_OPTIONS
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

    if let Some(ref tls_options) = CLIENT_OPTIONS.tls_options() {
        if let Some(true) = tls_options.allow_invalid_certificates {
            uri.push_str("&tlsAllowInvalidCertificates=true");
        }

        if let Some(ref ca_file_path) = tls_options.ca_file_path {
            uri.push_str("&tlsCAFile=");
            uri.push_str(
                &percent_encoding::utf8_percent_encode(
                    ca_file_path,
                    percent_encoding::NON_ALPHANUMERIC,
                )
                .to_string(),
            );
        }

        if let Some(ref cert_key_file_path) = tls_options.cert_key_file_path {
            uri.push_str("&tlsCertificateKeyFile=");
            uri.push_str(
                &percent_encoding::utf8_percent_encode(
                    cert_key_file_path,
                    percent_encoding::NON_ALPHANUMERIC,
                )
                .to_string(),
            );
        }
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn scram_sha1() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let client = TestClient::new().await;
    if !client.auth_enabled() {
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn scram_sha256() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let client = TestClient::new().await;
    if client.server_version_lt(4, 0) || !client.auth_enabled() {
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn scram_both() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let client = TestClient::new().await;
    if client.server_version_lt(4, 0) || !client.auth_enabled() {
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn scram_missing_user_uri() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let client = TestClient::new().await;
    if !client.auth_enabled() {
        return;
    }
    auth_test_uri("adsfasdf", "ASsdfsadf", None, false).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn scram_missing_user_options() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let client = TestClient::new().await;
    if !client.auth_enabled() {
        return;
    }
    auth_test_options("sadfasdf", "fsdadsfasdf", None, false).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn saslprep() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let client = TestClient::new().await;

    if client.server_version_lt(4, 0) || !client.auth_enabled() {
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn x509_auth() {
    let _guard: RwLockReadGuard<_> = LOCK.run_concurrently().await;

    let username = match std::env::var("MONGO_X509_USER") {
        Ok(user) => user,
        Err(_) => return,
    };

    let client = TestClient::new().await;
    let drop_user_result = client
        .database("$external")
        .run_command(doc! { "dropUser": &username }, None)
        .await;

    match drop_user_result.as_ref().map_err(|e| &e.kind) {
        Err(ErrorKind::CommandError(CommandError { code: 11, .. })) | Ok(_) => {}
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

    let mut options = CLIENT_OPTIONS.clone();
    options.credential = Some(
        Credential::builder()
            .mechanism(AuthMechanism::MongoDbX509)
            .build(),
    );

    let client = TestClient::with_options(Some(options)).await;
    client
        .database(function_name!())
        .collection::<Document>(function_name!())
        .find_one(None, None)
        .await
        .unwrap();
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn plain_auth() {
    let _guard: RwLockReadGuard<_> = LOCK.run_concurrently().await;

    if std::env::var("MONGO_PLAIN_AUTH_TEST").is_err() {
        return;
    }

    let options = ClientOptions::builder()
        .hosts(vec![StreamAddress {
            hostname: "ldaptest.10gen.cc".into(),
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

    let doc = coll.find_one(None, None).await.unwrap().unwrap();

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
