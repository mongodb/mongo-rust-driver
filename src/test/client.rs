use std::{borrow::Cow, collections::HashMap, time::Duration};

use bson::{bson, doc, Bson};
use serde::Deserialize;

use crate::{
    error::{Error, ErrorKind},
    options::{
        auth::{AuthMechanism, Credential},
        ClientOptions,
    },
    selection_criteria::{ReadPreference, SelectionCriteria},
    test::{CLIENT, LOCK},
    Client,
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
    let db = CLIENT.database("admin");
    let result = db.run_command(doc! { "currentOp": 1 }, None).unwrap();

    let in_prog = match result.get("inprog") {
        Some(Bson::Array(in_prog)) => in_prog,
        _ => panic!("no `inprog` array found in response to `currentOp`"),
    };

    let metadata: Metadata = bson::from_bson(in_prog[0].clone()).unwrap();
    assert_eq!(metadata.client.driver.name, "mrd");
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn server_selection_timeout_message() {
    let _guard = LOCK.run_concurrently();

    if !CLIENT.is_replica_set() {
        return;
    }

    let mut tag_set = HashMap::new();
    tag_set.insert("asdfasdf".to_string(), "asdfadsf".to_string());

    let unsatisfiable_read_preference = ReadPreference::Secondary {
        tag_sets: Some(vec![tag_set]),
        max_staleness: None,
    };

    let mut options = CLIENT.options.clone();
    options.server_selection_timeout = Some(Duration::from_millis(500));

    let client = Client::with_options(options).unwrap();
    let db = client.database("test");
    let error = db
        .run_command(
            doc! { "isMaster": 1 },
            SelectionCriteria::ReadPreference(unsatisfiable_read_preference),
        )
        .expect_err("should fail with server selection timeout error");

    let error_description = format!("{}", error);
    for host in CLIENT.options.hosts.iter() {
        assert!(error_description.contains(format!("{}", host).as_str()));
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn list_databases() {
    let _guard = LOCK.run_concurrently();

    let expected_dbs = &[
        format!("{}1", function_name!()),
        format!("{}2", function_name!()),
        format!("{}3", function_name!()),
    ];

    for name in expected_dbs {
        CLIENT.database(name).drop(None).unwrap();
    }

    let prev_dbs = CLIENT.list_databases(None).unwrap();

    for name in expected_dbs {
        assert!(!prev_dbs
            .iter()
            .any(|doc| doc.get("name") == Some(&Bson::String(name.to_string()))));

        let db = CLIENT.database(name);

        db.collection("foo")
            .insert_one(doc! { "x": 1 }, None)
            .unwrap();
    }

    let new_dbs = CLIENT.list_databases(None).unwrap();
    let new_dbs: Vec<_> = new_dbs
        .into_iter()
        .filter(|doc| match doc.get("name") {
            Some(&Bson::String(ref name)) => expected_dbs.contains(name),
            _ => false,
        })
        .collect();
    assert_eq!(new_dbs.len(), expected_dbs.len());

    for name in expected_dbs {
        let db_doc = new_dbs
            .iter()
            .find(|doc| doc.get("name") == Some(&Bson::String(name.to_string())))
            .unwrap();
        assert!(db_doc.contains_key("sizeOnDisk"));
        assert!(db_doc.contains_key("empty"));
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn list_database_names() {
    let _guard = LOCK.run_concurrently();

    let expected_dbs = &[
        format!("{}1", function_name!()),
        format!("{}2", function_name!()),
        format!("{}3", function_name!()),
    ];

    for name in expected_dbs {
        CLIENT.database(name).drop(None).unwrap();
    }

    let prev_dbs = CLIENT.list_database_names(None).unwrap();

    for name in expected_dbs {
        assert!(!prev_dbs.iter().any(|db_name| db_name == name));

        let db = CLIENT.database(name);

        db.collection("foo")
            .insert_one(doc! { "x": 1 }, None)
            .unwrap();
    }

    let new_dbs = CLIENT.list_database_names(None).unwrap();

    for name in expected_dbs {
        assert_eq!(new_dbs.iter().filter(|db_name| db_name == &name).count(), 1);
    }
}

fn is_auth_error(error: Error) -> bool {
    match error.kind.as_ref() {
        ErrorKind::AuthenticationError { .. } => true,
        _ => false,
    }
}

/// Performs an operation that requires authentication and verifies that it either succeeded or
/// failed with an authentication error according to the `should_succeed` parameter.
fn auth_test(client: Client, should_succeed: bool) {
    let result = client.list_database_names(None);
    if should_succeed {
        result.unwrap();
    } else {
        assert!(is_auth_error(result.unwrap_err()));
    }
}

/// Attempts to authenticate using the given username/password, optionally specifying a mechanism
/// via the `ClientOptions` api.
///
/// Asserts that the authentication's success matches the provided parameter.
fn auth_test_options(user: &str, password: &str, mechanism: Option<AuthMechanism>, success: bool) {
    let options = ClientOptions::builder()
        .hosts(CLIENT.options.hosts.clone())
        .max_pool_size(1)
        .credential(Credential {
            username: Some(user.to_string()),
            password: Some(password.to_string()),
            mechanism,
            ..Default::default()
        })
        .tls(CLIENT.options.tls.clone())
        .build();

    auth_test(Client::with_options(options).unwrap(), success);
}

/// Attempts to authenticate using the given username/password, optionally specifying a mechanism
/// via the URI api.
///
/// Asserts that the authentication's success matches the provided parameter.
fn auth_test_uri(
    user: &str,
    password: &str,
    mechanism: Option<AuthMechanism>,
    should_succeed: bool,
) {
    let host = CLIENT
        .options
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

    if let Some(ref tls_options) = CLIENT.options.tls_options() {
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

    auth_test(Client::with_uri_str(uri.as_str()).unwrap(), should_succeed);
}

/// Tries to authenticate with the given credentials using the given mechanisms, both by explicitly
/// specifying each mechanism and by relying on mechanism negotiation.
///
/// If only one mechanism is supplied, this will also test that using the other SCRAM mechanism will
/// fail.
fn scram_test(username: &str, password: &str, mechanisms: &[AuthMechanism]) {
    let _guard = LOCK.run_concurrently();

    for mechanism in mechanisms {
        auth_test_uri(username, password, Some(mechanism.clone()), true);
        auth_test_uri(username, password, None, true);
        auth_test_options(username, password, Some(mechanism.clone()), true);
        auth_test_options(username, password, None, true);
    }

    // If only one scram mechanism is specified, verify the other doesn't work.
    if mechanisms.len() == 1 && CLIENT.server_version_gte(4, 0) {
        let other = match mechanisms[0] {
            AuthMechanism::ScramSha1 => AuthMechanism::ScramSha256,
            _ => AuthMechanism::ScramSha1,
        };
        auth_test_uri(username, password, Some(other.clone()), false);
        auth_test_options(username, password, Some(other), false);
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn scram_sha1() {
    if !CLIENT.auth_enabled() {
        return;
    }

    CLIENT
        .create_user("sha1", "sha1", &["root"], &[AuthMechanism::ScramSha1])
        .unwrap();
    scram_test("sha1", "sha1", &[AuthMechanism::ScramSha1]);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn scram_sha256() {
    if CLIENT.server_version_lt(4, 0) || !CLIENT.auth_enabled() {
        return;
    }
    CLIENT
        .create_user("sha256", "sha256", &["root"], &[AuthMechanism::ScramSha256])
        .unwrap();
    scram_test("sha256", "sha256", &[AuthMechanism::ScramSha256]);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn scram_both() {
    if CLIENT.server_version_lt(4, 0) || !CLIENT.auth_enabled() {
        return;
    }
    CLIENT
        .create_user(
            "both",
            "both",
            &["root"],
            &[AuthMechanism::ScramSha1, AuthMechanism::ScramSha256],
        )
        .unwrap();
    scram_test(
        "both",
        "both",
        &[AuthMechanism::ScramSha1, AuthMechanism::ScramSha256],
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn scram_missing_user_uri() {
    if !CLIENT.auth_enabled() {
        return;
    }
    auth_test_uri("adsfasdf", "ASsdfsadf", None, false);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn scram_missing_user_options() {
    if !CLIENT.auth_enabled() {
        return;
    }
    auth_test_options("sadfasdf", "fsdadsfasdf", None, false);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn saslprep_options() {
    if CLIENT.server_version_lt(4, 0) || !CLIENT.auth_enabled() {
        return;
    }

    CLIENT
        .create_user("IX", "IX", &["root"], &[AuthMechanism::ScramSha256])
        .unwrap();
    CLIENT
        .create_user(
            "\u{2168}",
            "\u{2163}",
            &["root"],
            &[AuthMechanism::ScramSha256],
        )
        .unwrap();

    auth_test_options("IX", "IX", None, true);
    auth_test_options("IX", "I\u{00AD}X", None, true);
    auth_test_options("\u{2168}", "IV", None, true);
    auth_test_options("\u{2168}", "I\u{00AD}V", None, true);
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn saslprep_uri() {
    if CLIENT.server_version_lt(4, 0) || !CLIENT.auth_enabled() {
        return;
    }

    CLIENT
        .create_user("IX", "IX", &["root"], &[AuthMechanism::ScramSha256])
        .unwrap();
    CLIENT
        .create_user(
            "\u{2168}",
            "\u{2163}",
            &["root"],
            &[AuthMechanism::ScramSha256],
        )
        .unwrap();

    auth_test_uri("IX", "IX", None, true);
    auth_test_uri("IX", "I%C2%ADX", None, true);
    auth_test_uri("%E2%85%A8", "IV", None, true);
    auth_test_uri("%E2%85%A8", "I%C2%ADV", None, true);
}
