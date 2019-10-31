use bson::{Bson, Document};
use serde::Deserialize;

use crate::{
    client::options::{ClientOptions, StreamAddress},
    error::ErrorKind,
    read_preference::ReadPreference,
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

fn sort_document(document: &mut Document) {
    let temp = std::mem::replace(document, Default::default());

    let mut elements: Vec<_> = temp.into_iter().collect();
    elements.sort_by(|e1, e2| e1.0.cmp(&e2.0));

    document.extend(elements);
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
        doc.insert("connecttimeoutms", i.as_millis() as i64);
    }

    if let Some(i) = options.heartbeat_freq.take() {
        doc.insert("heartbeatfrequencyms", i.as_millis() as i64);
    }

    if let Some(i) = options.local_threshold.take() {
        doc.insert("localthresholdms", i);
    }

    if let Some(i) = options.max_idle_time.take() {
        doc.insert("maxidletimems", i.as_millis() as i64);
    }

    if let Some(s) = options.repl_set_name.take() {
        doc.insert("replicaset", s);
    }

    if let Some(read_pref) = options.read_preference.take() {
        let (level, tag_sets, max_staleness) = match read_pref {
            ReadPreference::Primary => ("primary", None, None),
            ReadPreference::PrimaryPreferred {
                tag_sets,
                max_staleness,
            } => ("primaryPreferred", tag_sets, max_staleness),
            ReadPreference::Secondary {
                tag_sets,
                max_staleness,
            } => ("secondary", tag_sets, max_staleness),
            ReadPreference::SecondaryPreferred {
                tag_sets,
                max_staleness,
            } => ("secondaryPreferred", tag_sets, max_staleness),
            ReadPreference::Nearest {
                tag_sets,
                max_staleness,
            } => ("nearest", tag_sets, max_staleness),
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
    }

    if let Some(i) = options.max_staleness.take() {
        doc.insert("maxstalenessseconds", i.as_millis() as i64);
    }

    if let Some(b) = options.retry_reads.take() {
        doc.insert("retryreads", b);
    }

    if let Some(b) = options.retry_writes.take() {
        doc.insert("retrywrites", b);
    }

    if let Some(i) = options.server_selection_timeout.take() {
        doc.insert("serverselectiontimeoutms", i.as_millis() as i64);
    }

    if let Some(i) = options.socket_timeout.take() {
        doc.insert("sockettimeoutms", i.as_millis() as i64);
    }

    if let Some(mut opt) = options.tls_options.take() {
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
        doc.insert("readconcernlevel", s.as_str());
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
        doc.insert("wtimeoutms", i.as_millis() as i64);
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
        doc.insert("zlibcompressionlevel", i64::from(i));
    }

    doc
}

fn run_test(test_file: TestFile) {
    for mut test_case in test_file.tests {
        // TODO: RUST-226: Investigate whether tlsCertificateKeyFilePassword is supported in rustls
        if test_case.description.contains("tlsAllowInvalidHostnames")
            || test_case.description.contains("single-threaded")
            || test_case.description.contains("serverSelectionTryOnce")
            || test_case
                .description
                .contains("tlsCertificateKeyFilePassword")
            || test_case.description.contains("ipv6")
            || test_case.description.contains("Unix")
            || test_case.description.contains("MONGODB-CR")
            || test_case.description.contains("X509")
            || test_case.description.contains("GSSAPI")
            || test_case.description.contains("IP literal")
            || test_case.description.contains("relative path")
        {
            continue;
        }

        let warning = if let Some(warn_val) = test_case.warning.take() {
            warn_val
        } else {
            false
        };

        if test_case.valid && !warning {
            let mut is_unsupported_host_type = false;
            // hosts
            if let Some(mut json_hosts) = test_case.hosts.take() {
                // skip over unsupported host types
                is_unsupported_host_type = json_hosts.iter_mut().any(|h_json| {
                    match h_json.remove("type").as_ref().and_then(Bson::as_str) {
                        Some("ip_literal") | Some("unix") => true,
                        _ => false,
                    }
                });

                if !is_unsupported_host_type {
                    let options = ClientOptions::parse(&test_case.uri).unwrap();
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
                let options = ClientOptions::parse(&test_case.uri).unwrap();
                let mut options_doc = document_from_client_options(options);
                if let Some(json_options) = test_case.options {
                    let mut json_options: Document = json_options
                        .into_iter()
                        .map(|(k, v)| (k.to_lowercase(), v))
                        .collect();

                    if !json_options.contains_key("tlsallowinvalidcertificates") {
                        if let Some(val) = json_options.remove("tlsinsecure") {
                            json_options
                                .insert("tlsallowinvalidcertificates", !val.as_bool().unwrap());
                        }
                    }

                    if !json_options.contains_key("authsource") {
                        options_doc.remove("authsource");
                    }

                    assert_eq!(options_doc, json_options)
                }
                // auth
                if let Some(json_auth) = test_case.auth {
                    // skip over unsupported auth types
                    if let Some(auth_mechanism) = options_doc.get("authmechanism") {
                        if auth_mechanism.as_str() == Some("MONGODB-CR") {
                            continue;
                        }
                    }
                    let mut options = ClientOptions::parse(&test_case.uri).unwrap();
                    if let Some(credential) = options.credential.take() {
                        let auth_doc = credential.into_document();
                        assert_eq!(auth_doc, json_auth);
                    }
                }
            }
        } else {
            let expected_type = if warning { "warning" } else { "error" };

            match ClientOptions::parse(&test_case.uri).map_err(ErrorKind::from) {
                Ok(_) => panic!("expected {}", expected_type),
                Err(ErrorKind::ArgumentError(s)) => {}
                Err(e) => panic!("expected ArgumentError, but got {:?}", e),
            }
        }
    }
}

#[test]
fn run_uri_options_spec_tests() {
    crate::test::run(&["uri-options"], run_test);
}

#[test]
fn run_connection_string_spec_tests() {
    crate::test::run(&["connection-string"], run_test);
}
