use pretty_assertions::assert_eq;
use serde::Deserialize;

use crate::{
    bson::{Bson, Document},
    client::options::{ClientOptions, ClientOptionsParser, StreamAddress},
    error::ErrorKind,
    selection_criteria::{ReadPreference, SelectionCriteria},
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

fn document_from_client_options(mut options: ClientOptions) -> Document {
    let mut doc = Document::new();

    if let Some(s) = options.app_name.take() {
        doc.insert("appname", s);
    }

    if let Some(mechanism) = options
        .credential
        .get_or_insert_with(Default::default)
        .mechanism
        .take()
    {
        doc.insert("authmechanism", mechanism.as_str().to_string());
    }

    if let Some(d) = options
        .credential
        .get_or_insert_with(Default::default)
        .mechanism_properties
        .take()
    {
        doc.insert("authmechanismproperties", d);
    }

    if let Some(s) = options
        .credential
        .get_or_insert_with(Default::default)
        .source
        .take()
    {
        doc.insert("authsource", s);
    }

    if let Some(i) = options.connect_timeout.take() {
        doc.insert("connecttimeoutms", i.as_millis() as i32);
    }

    if let Some(b) = options.direct_connection.take() {
        doc.insert("directconnection", b);
    }

    if let Some(i) = options.heartbeat_freq.take() {
        doc.insert("heartbeatfrequencyms", i.as_millis() as i32);
    }

    if let Some(i) = options.local_threshold.take() {
        doc.insert("localthresholdms", i.as_millis() as i32);
    }

    if let Some(i) = options.max_idle_time.take() {
        doc.insert("maxidletimems", i.as_millis() as i32);
    }

    if let Some(i) = options.max_pool_size.take() {
        doc.insert("maxpoolsize", i as i32);
    }

    if let Some(i) = options.min_pool_size.take() {
        doc.insert("minpoolsize", i as i32);
    }

    if let Some(s) = options.repl_set_name.take() {
        doc.insert("replicaset", s);
    }

    if let Some(SelectionCriteria::ReadPreference(read_pref)) = options.selection_criteria.take() {
        let (level, tag_sets, max_staleness) = match read_pref {
            ReadPreference::Primary => ("primary", None, None),
            ReadPreference::PrimaryPreferred { options } => {
                ("primaryPreferred", options.tag_sets, options.max_staleness)
            }
            ReadPreference::Secondary { options } => {
                ("secondary", options.tag_sets, options.max_staleness)
            }
            ReadPreference::SecondaryPreferred { options } => (
                "secondaryPreferred",
                options.tag_sets,
                options.max_staleness,
            ),
            ReadPreference::Nearest { options } => {
                ("nearest", options.tag_sets, options.max_staleness)
            }
        };

        doc.insert("readpreference", level);

        if let Some(tag_sets) = tag_sets {
            let tags: Vec<Bson> = tag_sets
                .into_iter()
                .map(|tag_set| {
                    let mut tag_set: Vec<_> = tag_set.into_iter().collect();
                    tag_set.sort();
                    Bson::Document(tag_set.into_iter().map(|(k, v)| (k, v.into())).collect())
                })
                .collect();

            doc.insert("readpreferencetags", tags);
        }

        if let Some(i) = max_staleness {
            doc.insert("maxstalenessseconds", i.as_secs() as i32);
        }
    }

    if let Some(b) = options.retry_reads.take() {
        doc.insert("retryreads", b);
    }

    if let Some(b) = options.retry_writes.take() {
        doc.insert("retrywrites", b);
    }

    if let Some(i) = options.server_selection_timeout.take() {
        doc.insert("serverselectiontimeoutms", i.as_millis() as i32);
    }

    if let Some(i) = options.socket_timeout.take() {
        doc.insert("sockettimeoutms", i.as_millis() as i32);
    }

    if let Some(mut opt) = options.tls_options() {
        let ca_file_path = opt.ca_file_path.take();
        let cert_key_file_path = opt.cert_key_file_path.take();
        let allow_invalid_certificates = opt.allow_invalid_certificates.take();

        if let Some(s) = ca_file_path {
            doc.insert("tls", true);
            doc.insert("tlscafile", s);
        }

        if let Some(s) = cert_key_file_path {
            doc.insert("tlscertificatekeyfile", s);
        }

        if let Some(b) = allow_invalid_certificates {
            doc.insert("tlsallowinvalidcertificates", b);
        }
    }

    if let Some(vec) = options.compressors.take() {
        doc.insert(
            "compressors",
            Bson::Array(vec.into_iter().map(Bson::String).collect()),
        );
    }

    if let Some(s) = options.read_concern.take() {
        doc.insert("readconcernlevel", s.level.as_str());
    }

    if let Some(i_or_s) = options
        .write_concern
        .get_or_insert_with(Default::default)
        .w
        .take()
    {
        doc.insert("w", i_or_s.to_bson());
    }

    if let Some(i) = options
        .write_concern
        .get_or_insert_with(Default::default)
        .w_timeout
        .take()
    {
        doc.insert("wtimeoutms", i.as_millis() as i32);
    }

    if let Some(b) = options
        .write_concern
        .get_or_insert_with(Default::default)
        .journal
        .take()
    {
        doc.insert("journal", b);
    }

    if let Some(i) = options.zlib_compression.take() {
        doc.insert("zlibcompressionlevel", i);
    }

    doc
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
                        .map(StreamAddress::into_document)
                        .collect();

                    assert_eq!(hosts, json_hosts);
                }
            }
            if !is_unsupported_host_type {
                // options
                let options = ClientOptions::parse(&test_case.uri)
                    .await
                    .expect(&test_case.description);
                let mut options_doc = document_from_client_options(options);
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
                .as_ref()
                .map_err(|e| &e.kind)
            {
                Ok(_) => panic!("expected {}", expected_type),
                Err(ErrorKind::ArgumentError { .. }) => {}
                Err(e) => panic!("expected ArgumentError, but got {:?}", e),
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
        .as_ref()
        .map_err(|e| &e.kind)
    {
        Ok(_) => panic!("expected error for option {}", option),
        Err(ErrorKind::ArgumentError { message, .. }) => {
            match suggestion {
                Some(s) => assert!(message.contains(s)),
                None => assert!(!message.contains("similar")),
            };
        }
        Err(e) => panic!("expected ArgumentError, but got {:?}", e),
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
