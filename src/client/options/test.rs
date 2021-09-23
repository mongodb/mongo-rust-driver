use pretty_assertions::assert_eq;
use serde::Deserialize;

use crate::{
    bson::{Bson, Document},
    client::options::{ClientOptions, ClientOptionsParser, ServerAddress},
    error::ErrorKind,
    test::run_spec_test,
};
#[derive(Debug, Deserialize)]
struct TestFile {
    pub tests: Vec<TestCase>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TestCase {
    pub description: String,
    pub uri: String,
    pub valid: bool,
    pub warning: Option<bool>,
    pub hosts: Option<Vec<Document>>,
    pub auth: Option<Document>,
    pub options: Option<Document>,
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
            || test_case.description.contains("Unix")
            || test_case.description.contains("relative path")
        {
            continue;
        }

        let warning = test_case.warning.take().unwrap_or(false);

        if test_case.valid && !warning {
            let mut is_unsupported_host_type = false;
            // hosts
            if let Some(mut json_hosts) = test_case.hosts.take() {
                // skip over unsupported host types
                is_unsupported_host_type = json_hosts.iter_mut().any(|h_json| {
                    matches!(
                        h_json.remove("type").as_ref().and_then(Bson::as_str),
                        Some("ip_literal") | Some("unix")
                    )
                });

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
                let mut options_doc = bson::to_document(dbg!(&options)).unwrap_or_else(|_| {
                    panic!(
                        "{}: Failed to serialize ClientOptions",
                        &test_case.description
                    )
                });
                dbg!(&options_doc);
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

                    options_doc = options_doc
                        .into_iter()
                        .filter(|(ref key, _)| json_options.contains_key(key))
                        .collect();

                    assert_eq!(options_doc, json_options, "{}", test_case.description)
                }
                // auth
                if let Some(json_auth) = test_case.auth {
                    let json_auth: Document = json_auth
                        .into_iter()
                        .filter_map(|(k, v)| {
                            if let Bson::Null = v {
                                None
                            } else {
                                Some((k.to_lowercase(), v))
                            }
                        })
                        .collect();

                    let options = ClientOptions::parse(&test_case.uri).await.unwrap();
                    let mut expected_auth = options.credential.unwrap_or_default().into_document();
                    expected_auth = expected_auth
                        .into_iter()
                        .filter(|(ref key, _)| json_auth.contains_key(key))
                        .collect();

                    assert_eq!(expected_auth, json_auth);
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_uri_options_spec_tests() {
    run_spec_test(&["uri-options"], run_test).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_connection_string_spec_tests() {
    run_spec_test(&["connection-string"], run_test).await;
}

async fn parse_uri(option: &str, suggestion: Option<&str>) {
    match ClientOptionsParser::parse(&format!("mongodb://host:27017/?{}=test", option))
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
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
