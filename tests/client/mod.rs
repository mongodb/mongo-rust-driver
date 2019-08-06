use std::{borrow::Cow, time::Duration};

use bson::Bson;

use mongodb::{
    error::{Error, ErrorKind},
    options::{
        auth::{AuthMechanism, Credential},
        ClientOptions,
    },
    Client,
};

use crate::CLIENT;

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
// #[test]
#[allow(unused)]
fn metadata_sent_in_handshake() {
    let db = CLIENT.database("admin");
    let result = db.run_command(doc! { "currentOp": 1 }, None).unwrap();

    let in_prog = match result.get("inprog") {
        Some(Bson::Array(in_prog)) => in_prog,
        _ => panic!("no `inprog` array found in response to `currentOp`"),
    };

    let metadata: Metadata = bson::from_bson(in_prog[0].clone()).unwrap();
    assert_eq!(metadata.client.driver.name, "mrd");
}

#[test]
#[function_name]
fn list_databases() {
    let expected_dbs = &[
        format!("{}1", function_name!()),
        format!("{}2", function_name!()),
        format!("{}3", function_name!()),
    ];

    for name in expected_dbs {
        CLIENT.database(name).drop().unwrap();
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

#[test]
#[function_name]
fn list_database_names() {
    let expected_dbs = &[
        format!("{}1", function_name!()),
        format!("{}2", function_name!()),
        format!("{}3", function_name!()),
    ];

    for name in expected_dbs {
        CLIENT.database(name).drop().unwrap();
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

/// Brief connectTimeoutMS used to shorten the SCRAM tests. Can remove once dependency on R2D2 is
/// removed.
const CONNECT_TIMEOUT: u64 = 1000;

/// Authentication errors returned as part of connection establishment are consumed by R2D2 then
/// converted into different `ErrorKind`'s by error_chain. For now, we simply dig down to the
/// description and check if it contains "Authentication". Once R2D2 is removed, this function can
/// go away and we can just check for our own auth error types.
fn is_auth_r2d2_error(e: Error) -> bool {
    match e {
        Error(ErrorKind::R2D2(e), _) => e.to_string().contains("Authentication"),
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
        assert!(is_auth_r2d2_error(result.unwrap_err()));
    }
}

/// Attempts to authenticate using the given username/password, optionally specifying a mechanism,
/// via the `ClientOptions` api.
///
/// Asserts that the authentication's success matches the provided parameter.
fn auth_test_options(user: &str, password: &str, mechanism: Option<AuthMechanism>, success: bool) {
    let options = ClientOptions {
        hosts: CLIENT.options.hosts.clone(),
        max_pool_size: Some(1),
        connect_timeout: Some(Duration::from_millis(CONNECT_TIMEOUT)),
        credential: Some(Credential {
            username: Some(user.to_string()),
            password: Some(password.to_string()),
            mechanism,
            ..Default::default()
        }),
        ..Default::default()
    };
    auth_test(Client::with_options(options).unwrap(), success);
}

/// Attempts to authenticate using the given username/password, optionally specifying a mechanism,
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
    let uri = format!(
        "mongodb://{}:{}@{}/?maxPoolSize=1&connectTimeoutMS={}{}",
        user,
        password,
        host,
        CONNECT_TIMEOUT,
        mechanism_str.as_ref()
    );
    auth_test(Client::with_uri_str(uri.as_str()).unwrap(), should_succeed);
}

/// Tries to authenticate with the given credentials using the given mechanisms, both by explicitly
/// specifying each mechanism and by relying on mechanism negotiation.
///
/// If only one mechanism is supplied, this will also test that using the other SCRAM mechanism will
/// fail.
fn scram_test(username: &str, password: &str, mechanisms: &[AuthMechanism]) {
    for mechanism in mechanisms {
        auth_test_uri(username, password, Some(mechanism.clone()), true);
        auth_test_uri(username, password, None, true);
        auth_test_options(username, password, Some(mechanism.clone()), true);
        auth_test_options(username, password, None, true);
    }

    // If only one scram mechanism is specified, verify the other doesn't work.
    if mechanisms.len() == 1 {
        let other = match mechanisms[0] {
            AuthMechanism::ScramSha1 => AuthMechanism::ScramSha256,
            _ => AuthMechanism::ScramSha1,
        };
        auth_test_uri(username, password, Some(other.clone()), false);
        auth_test_options(username, password, Some(other), false);
    }
}

#[test]
fn scram_sha1() {
    if !CLIENT.version_at_least_40() || !CLIENT.auth_enabled() {
        return;
    }
    CLIENT
        .create_user("sha1", "sha1", &["root"], &[AuthMechanism::ScramSha1])
        .unwrap();
    scram_test("sha1", "sha1", &[AuthMechanism::ScramSha1]);
}

#[test]
fn scram_sha256() {
    if !CLIENT.version_at_least_40() || !CLIENT.auth_enabled() {
        return;
    }
    CLIENT
        .create_user("sha256", "sha256", &["root"], &[AuthMechanism::ScramSha256])
        .unwrap();
    scram_test("sha256", "sha256", &[AuthMechanism::ScramSha256]);
}

#[test]
fn scram_both() {
    if !CLIENT.version_at_least_40() || !CLIENT.auth_enabled() {
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

#[test]
fn scram_missing_user_uri() {
    if !CLIENT.version_at_least_40() || !CLIENT.auth_enabled() {
        return;
    }
    auth_test_uri("adsfasdf", "ASsdfsadf", None, false);
}

#[test]
fn scram_missing_user_options() {
    if !CLIENT.version_at_least_40() || !CLIENT.auth_enabled() {
        return;
    }
    auth_test_options("sadfasdf", "fsdadsfasdf", None, false);
}

#[test]
fn saslprep_options() {
    if !CLIENT.version_at_least_40() || !CLIENT.auth_enabled() {
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

#[test]
fn saslprep_uri() {
    if !CLIENT.version_at_least_40() || !CLIENT.auth_enabled() {
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
