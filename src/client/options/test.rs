use std::time::Duration;

use bson::UuidRepresentation;
use once_cell::sync::Lazy;
use pretty_assertions::assert_eq;
use serde::Deserialize;

use crate::{
    bson::{Bson, Document},
    bson_util::get_int,
    client::options::{ClientOptions, ConnectionString, ServerAddress},
    error::ErrorKind,
    test::spec::deserialize_spec_tests,
    Client,
};

static SKIPPED_TESTS: Lazy<Vec<&'static str>> = Lazy::new(|| {
    let mut skipped_tests = vec![
        // TODO RUST-1309: unskip this test
        "tlsInsecure is parsed correctly",
        // The driver does not support maxPoolSize=0
        "maxPoolSize=0 does not error",
        // TODO RUST-226: unskip this test
        "Valid tlsCertificateKeyFilePassword is parsed correctly",
    ];

    // TODO RUST-1896: unskip this test when openssl-tls is enabled
    // if cfg!(not(feature = "openssl-tls"))
    skipped_tests.push("tlsAllowInvalidHostnames is parsed correctly");
    // }

    if cfg!(not(feature = "zlib-compression")) {
        skipped_tests.push("Valid compression options are parsed correctly");
        skipped_tests.push("Non-numeric zlibCompressionLevel causes a warning");
        skipped_tests.push("Too low zlibCompressionLevel causes a warning");
        skipped_tests.push("Too high zlibCompressionLevel causes a warning");
    }

    if cfg!(not(all(
        feature = "zlib-compression",
        feature = "snappy-compression"
    ))) {
        skipped_tests.push("Multiple compressors are parsed correctly");
    }

    skipped_tests
});

#[derive(Debug, Deserialize)]
struct TestFile {
    pub tests: Vec<TestCase>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TestCase {
    description: String,
    uri: String,
    valid: bool,
    warning: Option<bool>,
    hosts: Option<Vec<ServerAddress>>,
    auth: Option<TestAuth>,
    options: Option<Document>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TestAuth {
    username: Option<String>,
    password: Option<String>,
    db: Option<String>,
}

impl TestAuth {
    fn matches_client_options(&self, options: &ClientOptions) -> bool {
        let credential = options.credential.as_ref();
        self.username.as_ref() == credential.and_then(|cred| cred.username.as_ref())
            && self.password.as_ref() == credential.and_then(|cred| cred.password.as_ref())
            && self.db.as_ref() == options.default_database.as_ref()
    }
}

async fn run_tests(path: &[&str], skipped_files: &[&str]) {
    let test_files = deserialize_spec_tests::<TestFile>(path, Some(skipped_files))
        .into_iter()
        .map(|(test_file, _)| test_file);

    for test_file in test_files {
        for test_case in test_file.tests {
            if SKIPPED_TESTS.contains(&test_case.description.as_str()) {
                continue;
            }

            let client_options_result = ClientOptions::parse(&test_case.uri).await;

            // The driver does not log warnings for unsupported or incorrect connection string
            // values, so expect an error when warning is set to true.
            if test_case.valid && test_case.warning != Some(true) {
                let client_options = client_options_result.expect(&test_case.description);

                if let Some(ref expected_hosts) = test_case.hosts {
                    assert_eq!(
                        &client_options.hosts, expected_hosts,
                        "{}",
                        test_case.description
                    );
                }

                let mut actual_options =
                    bson::to_document(&client_options).expect(&test_case.description);

                if let Some(mode) = actual_options.remove("mode") {
                    actual_options.insert("readPreference", mode);
                }

                if let Some(tags) = actual_options.remove("tagSets") {
                    actual_options.insert("readPreferenceTags", tags);
                }

                #[cfg(any(
                    feature = "zstd-compression",
                    feature = "zlib-compression",
                    feature = "snappy-compression"
                ))]
                if let Some(ref compressors) = client_options.compressors {
                    use crate::options::Compressor;

                    actual_options.insert(
                        "compressors",
                        compressors
                            .iter()
                            .map(Compressor::name)
                            .collect::<Vec<&str>>(),
                    );

                    #[cfg(feature = "zlib-compression")]
                    if let Some(zlib_compression_level) = compressors
                        .iter()
                        .filter_map(|compressor| match compressor {
                            Compressor::Zlib { level } => *level,
                            _ => None,
                        })
                        .next()
                    {
                        actual_options.insert("zlibcompressionlevel", zlib_compression_level);
                    }
                }

                if let Some(ref expected_options) = test_case.options {
                    for (expected_key, expected_value) in expected_options {
                        if expected_value == &Bson::Null {
                            continue;
                        }

                        let (_, actual_value) = actual_options
                            .iter()
                            .find(|(actual_key, _)| {
                                actual_key.to_ascii_lowercase() == expected_key.to_ascii_lowercase()
                            })
                            .unwrap_or_else(|| {
                                panic!(
                                    "{}: parsed options missing {} key",
                                    test_case.description, expected_key
                                )
                            });

                        if let Some(expected_number) = get_int(expected_value) {
                            let actual_number = get_int(actual_value).unwrap_or_else(|| {
                                panic!(
                                    "{}: {} should be a numeric value but got {}",
                                    &test_case.description, expected_key, actual_value
                                )
                            });
                            assert_eq!(actual_number, expected_number, "{}", test_case.description);
                        } else {
                            assert_eq!(actual_value, expected_value, "{}", test_case.description);
                        }
                    }
                }

                if let Some(test_auth) = test_case.auth {
                    assert!(test_auth.matches_client_options(&client_options));
                }
            } else {
                let error = client_options_result.expect_err(&test_case.description);
                assert!(
                    matches!(*error.kind, ErrorKind::InvalidArgument { .. }),
                    "{}",
                    &test_case.description
                );
            }
        }
    }
}

#[tokio::test]
async fn run_uri_options_spec_tests() {
    let skipped_files = vec!["single-threaded-options.json"];
    run_tests(&["uri-options"], &skipped_files).await;
}

#[tokio::test]
async fn run_connection_string_spec_tests() {
    let mut skipped_files = Vec::new();
    if cfg!(not(unix)) {
        skipped_files.push("valid-unix_socket-absolute.json");
        skipped_files.push("valid-unix_socket-relative.json");
        // All the tests in this file use unix domain sockets
        skipped_files.push("valid-db-with-dotted-name.json");
    }

    run_tests(&["connection-string"], &skipped_files).await;
}

#[tokio::test]
async fn uuid_representations() {
    let mut uuid_repr = parse_uri_with_uuid_representation("csharpLegacy")
        .await
        .expect("expected `csharpLegacy` to be a valid argument for `uuidRepresentation`");
    assert_eq!(UuidRepresentation::CSharpLegacy, uuid_repr);

    uuid_repr = parse_uri_with_uuid_representation("javaLegacy")
        .await
        .expect("expected `javaLegacy` to be a valid argument for `uuidRepresentation`");
    assert_eq!(UuidRepresentation::JavaLegacy, uuid_repr);

    uuid_repr = parse_uri_with_uuid_representation("pythonLegacy")
        .await
        .expect("expected `pythonLegacy` to be a valid argument for `uuidRepresentation`");
    assert_eq!(UuidRepresentation::PythonLegacy, uuid_repr);

    let uuid_err = parse_uri_with_uuid_representation("unknownLegacy")
        .await
        .expect_err("expect `unknownLegacy` to be an invalid argument for `uuidRepresentation`");
    assert_eq!(
        "connection string `uuidRepresentation` option can be one of `csharpLegacy`, \
         `javaLegacy`, or `pythonLegacy`. Received invalid `unknownLegacy`"
            .to_string(),
        uuid_err
    );
}

async fn parse_uri_with_uuid_representation(
    uuid_repr: &str,
) -> std::result::Result<UuidRepresentation, String> {
    match ConnectionString::parse(format!(
        "mongodb://localhost:27017/?uuidRepresentation={}",
        uuid_repr
    ))
    .map_err(|e| e.message().unwrap())
    {
        Ok(cs) => Ok(cs.uuid_representation.unwrap()),
        Err(e) => Err(e),
    }
}

#[test]
fn parse_unknown_options() {
    fn parse_uri(option: &str, suggestion: Option<&str>) {
        match ConnectionString::parse(format!("mongodb://host:27017/?{}=test", option))
            .map_err(|e| *e.kind)
        {
            Ok(_) => panic!("expected error for option {}", option),
            Err(ErrorKind::InvalidArgument { message, .. }) => {
                match suggestion {
                    Some(s) => assert!(message.contains(s)),
                    None => assert!(!message.contains("similar")),
                };
            }
            Err(e) => panic!("expected InvalidArgument, but got {:?}", e),
        }
    }

    parse_uri("invalidoption", None);
    parse_uri("x", None);
    parse_uri("max", None);
    parse_uri("tlstimeout", None);
    parse_uri("waitqueuetimeout", Some("waitqueuetimeoutms"));
    parse_uri("retry_reads", Some("retryreads"));
    parse_uri("poolsize", Some("maxpoolsize"));
    parse_uri("maxstalenessms", Some("maxstalenessseconds"));
}

#[tokio::test]
async fn parse_with_no_default_database() {
    let uri = "mongodb://localhost/";

    assert_eq!(
        ClientOptions::parse(uri).await.unwrap(),
        ClientOptions {
            hosts: vec![ServerAddress::Tcp {
                host: "localhost".to_string(),
                port: None
            }],
            original_uri: Some(uri.into()),
            default_database: None,
            ..Default::default()
        }
    );
}

#[tokio::test]
async fn options_debug_omits_uri() {
    let uri = "mongodb://username:password@localhost/";
    let options = ClientOptions::parse(uri).await.unwrap();

    let debug_output = format!("{:?}", options);
    assert!(!debug_output.contains("username"));
    assert!(!debug_output.contains("password"));
    assert!(!debug_output.contains("uri"));
}

#[tokio::test]
async fn options_enforce_min_heartbeat_frequency() {
    let options = ClientOptions::builder()
        .hosts(vec![ServerAddress::parse("a:123").unwrap()])
        .heartbeat_freq(Duration::from_millis(10))
        .build();

    Client::with_options(options).unwrap_err();
}
