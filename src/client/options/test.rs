use std::time::Duration;

use bson::UuidRepresentation;
use pretty_assertions::assert_eq;
use serde::Deserialize;

#[cfg(any(
    feature = "zstd-compression",
    feature = "zlib-compression",
    feature = "snappy-compression"
))]
use crate::options::Compressor;
use crate::{
    bson::{Bson, Document},
    client::options::{ClientOptions, ConnectionString, ServerAddress},
    error::ErrorKind,
    test::run_spec_test,
    Client,
};
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
    hosts: Option<Vec<Document>>,
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

async fn run_test(test_file: TestFile) {
    for mut test_case in test_file.tests {
        if
        // TODO: RUST-229: Implement IPv6 Support
        test_case.description.contains("ipv6")
            || test_case.description.contains("IP literal")
            // TODO: RUST-226: Investigate whether tlsCertificateKeyFilePassword is supported in rustls
            || test_case
                .description
                .contains("tlsCertificateKeyFilePassword")
            // Not Implementing
            || test_case.description.contains("tlsAllowInvalidHostnames")
            || test_case.description.contains("single-threaded")
            || test_case.description.contains("serverSelectionTryOnce")
            || test_case.description.contains("relative path")
            // Compression is implemented but will only pass the tests if all
            // the appropriate feature flags are set.  That is because
            // valid compressors are only parsed correctly if the corresponding feature flag is set.
            // (otherwise they are treated as invalid, and hence ignored)
            || (test_case.description.contains("compress") &&
                    !cfg!(
                        all(features = "zlib-compression",
                            features = "zstd-compression",
                            features = "snappy-compression"
                        )
                    )
                )
            // The Rust driver disallows `maxPoolSize=0`.
            || test_case.description.contains("maxPoolSize=0 does not error")
            // TODO RUST-933 implement custom srvServiceName support
            || test_case.description.contains("custom srvServiceName")
        {
            continue;
        }

        #[cfg(not(unix))]
        if test_case.description.contains("Unix") {
            continue;
        }

        let warning = test_case.warning.take().unwrap_or(false);

        if test_case.valid && !warning {
            let mut is_unsupported_host_type = false;
            // hosts
            if let Some(mut json_hosts) = test_case.hosts.take() {
                // skip over unsupported host types
                #[cfg(not(unix))]
                {
                    is_unsupported_host_type = json_hosts.iter_mut().any(|h_json| {
                        matches!(
                            h_json.remove("type").as_ref().and_then(Bson::as_str),
                            Some("ip_literal") | Some("unix")
                        )
                    });
                }

                #[cfg(unix)]
                {
                    is_unsupported_host_type = json_hosts.iter_mut().any(|h_json| {
                        matches!(
                            h_json.remove("type").as_ref().and_then(Bson::as_str),
                            Some("ip_literal")
                        )
                    });
                }

                if !is_unsupported_host_type {
                    let options = ClientOptions::parse(&test_case.uri).await.unwrap();
                    let hosts: Vec<_> = options
                        .hosts
                        .into_iter()
                        .map(ServerAddress::into_document)
                        .collect();

                    assert_eq!(hosts, json_hosts);
                }
            }
            if !is_unsupported_host_type {
                // options
                let options = ClientOptions::parse(&test_case.uri)
                    .await
                    .expect(&test_case.description);
                let mut options_doc = bson::to_document(&options).unwrap_or_else(|_| {
                    panic!(
                        "{}: Failed to serialize ClientOptions",
                        &test_case.description
                    )
                });
                if let Some(json_options) = test_case.options {
                    let mut json_options: Document = json_options
                        .into_iter()
                        .filter_map(|(k, v)| {
                            if let Bson::Null = v {
                                None
                            } else {
                                Some((k.to_lowercase(), v))
                            }
                        })
                        .collect();

                    // tlsallowinvalidcertificates and tlsinsecure must be inverse of each other
                    if !json_options.contains_key("tlsallowinvalidcertificates") {
                        if let Some(val) = json_options.remove("tlsinsecure") {
                            json_options
                                .insert("tlsallowinvalidcertificates", !val.as_bool().unwrap());
                        }
                    }

                    // The default types parsed from the test file don't match those serialized
                    // from the `ClientOptions` struct.
                    if let Ok(min) = json_options.get_i32("minpoolsize") {
                        json_options.insert("minpoolsize", Bson::Int64(min.into()));
                    }
                    if let Ok(max) = json_options.get_i32("maxpoolsize") {
                        json_options.insert("maxpoolsize", Bson::Int64(max.into()));
                    }
                    if let Ok(max_connecting) = json_options.get_i32("maxconnecting") {
                        json_options.insert("maxconnecting", Bson::Int64(max_connecting.into()));
                    }

                    options_doc = options_doc
                        .into_iter()
                        .filter(|(ref key, _)| json_options.contains_key(key))
                        .collect();

                    // Compressor does not implement Serialize, so add the compressor names to the
                    // options manually.
                    #[cfg(any(
                        feature = "zstd-compression",
                        feature = "zlib-compression",
                        feature = "snappy-compression"
                    ))]
                    if let Some(compressors) = options.compressors {
                        options_doc.insert(
                            "compressors",
                            compressors
                                .iter()
                                .map(Compressor::name)
                                .collect::<Vec<&str>>(),
                        );
                        #[cfg(feature = "zlib-compression")]
                        for compressor in compressors {
                            if let Compressor::Zlib { level: Some(level) } = compressor {
                                options_doc.insert("zlibcompressionlevel", level);
                            }
                        }
                    }
                    assert_eq!(options_doc, json_options, "{}", test_case.description)
                }

                if let Some(test_auth) = test_case.auth {
                    let options = ClientOptions::parse(&test_case.uri).await.unwrap();
                    assert!(test_auth.matches_client_options(&options));
                }
            }
        } else {
            let expected_type = if warning { "warning" } else { "error" };

            match ClientOptions::parse(&test_case.uri)
                .await
                .map_err(|e| *e.kind)
            {
                Ok(_) => panic!("expected {}", expected_type),
                Err(ErrorKind::InvalidArgument { .. }) => {}
                Err(e) => panic!("expected InvalidArgument, but got {:?}", e),
            }
        }
    }
}

#[tokio::test]
async fn run_uri_options_spec_tests() {
    run_spec_test(&["uri-options"], run_test).await;
}

#[tokio::test]
async fn run_connection_string_spec_tests() {
    run_spec_test(&["connection-string"], run_test).await;
}

async fn parse_uri(option: &str, suggestion: Option<&str>) {
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

async fn parse_uri_with_uuid_representation(uuid_repr: &str) -> Result<UuidRepresentation, String> {
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

#[tokio::test]
async fn parse_unknown_options() {
    parse_uri("invalidoption", None).await;
    parse_uri("x", None).await;
    parse_uri("max", None).await;
    parse_uri("tlstimeout", None).await;
    parse_uri("waitqueuetimeout", Some("waitqueuetimeoutms")).await;
    parse_uri("retry_reads", Some("retryreads")).await;
    parse_uri("poolsize", Some("maxpoolsize")).await;
    parse_uri(
        "tlspermitinvalidcertificates",
        Some("tlsallowinvalidcertificates"),
    )
    .await;
    parse_uri("maxstalenessms", Some("maxstalenessseconds")).await;
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
