use bson::{Bson, Document};
use serde::Deserialize;

use crate::{client::options::ClientOptions, error::ErrorKind, read_preference::ReadPreference};

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
    pub warning: bool,
    pub options: Document,
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
        doc.insert("authMechanism", mechanism.as_str().to_string());
    }

    if let Some(d) = options
        .credential
        .get_or_insert_with(Default::default)
        .mechanism_properties
        .take()
    {
        doc.insert("authMechanismProperties", d);
    }

    if let Some(s) = options
        .credential
        .get_or_insert_with(Default::default)
        .source
        .take()
    {
        doc.insert("authSource", s);
    }

    if let Some(i) = options.connect_timeout.take() {
        doc.insert("connectTimeoutMS", i.as_millis() as i64);
    }

    if let Some(i) = options.heartbeat_freq.take() {
        doc.insert("heartbeatFrequencyMS", i.as_millis() as i64);
    }

    if let Some(i) = options.local_threshold.take() {
        doc.insert("localThresholdMS", i);
    }

    if let Some(i) = options.max_idle_time.take() {
        doc.insert("maxIdleTimeMS", i.as_millis() as i64);
    }

    if let Some(s) = options.repl_set_name.take() {
        doc.insert("replicaSet", s);
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

        doc.insert("readPreference", level);

        if let Some(tag_sets) = tag_sets {
            let tags: Vec<Bson> = tag_sets
                .into_iter()
                .map(|tag_set| {
                    let mut tag_set: Vec<_> = tag_set.into_iter().collect();
                    tag_set.sort();
                    Bson::Document(tag_set.into_iter().map(|(k, v)| (k, v.into())).collect())
                })
                .collect();

            doc.insert("readPreferenceTags", tags);
        }
    }

    if let Some(i) = options.max_staleness.take() {
        doc.insert("maxStalenessSeconds", i.as_millis() as i64);
    }

    if let Some(b) = options.retry_reads.take() {
        doc.insert("retryReads", b);
    }

    if let Some(b) = options.retry_writes.take() {
        doc.insert("retryWrites", b);
    }

    if let Some(i) = options.server_selection_timeout.take() {
        doc.insert("serverSelectionTimeoutMS", i.as_millis() as i64);
    }

    if let Some(i) = options.socket_timeout.take() {
        doc.insert("socketTimeoutMS", i.as_millis() as i64);
    }

    if let Some(mut opt) = options.tls_options.take() {
        let ca_file_path = opt.ca_file_path.take();
        let cert_key_file_path = opt.cert_key_file_path.take();
        let allow_invalid_certificates = opt.allow_invalid_certificates.take();

        if let Some(s) = ca_file_path {
            doc.insert("tls", true);
            doc.insert("tlsCAFile", s);
        }

        if let Some(s) = cert_key_file_path {
            doc.insert("tlsCertificateKeyFile", s);
        }

        if let Some(b) = allow_invalid_certificates {
            doc.insert("tlsAllowInvalidCertificates", b);
        }
    }

    if let Some(vec) = options.compressors.take() {
        doc.insert(
            "compressors",
            Bson::Array(vec.into_iter().map(Bson::String).collect()),
        );
    }

    if let Some(s) = options.read_concern.take() {
        doc.insert("readConcernLevel", s.as_str());
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
        doc.insert("wTimeoutMS", i.as_millis() as i64);
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
        doc.insert("zlibCompressionLevel", i64::from(i));
    }

    doc
}

fn run_test(test_file: TestFile) {
    for test_case in test_file.tests {
        // TODO: RUST-226
        if test_case.description.contains("tlsAllowInvalidHostnames")
            || test_case.description.contains("single-threaded")
            || test_case.description.contains("serverSelectionTryOnce")
            || test_case
                .description
                .contains("tlsCertificateKeyFilePassword")
        {
            continue;
        }

        let mut json_options = test_case.options;
        if !json_options.contains_key("tlsAllowInvalidCertificates") {
            if let Some(val) = json_options.remove("tlsInsecure") {
                json_options.insert("tlsAllowInvalidCertificates", !val.as_bool().unwrap());
            }
        }

        if test_case.valid && !test_case.warning {
            let options = ClientOptions::parse(&test_case.uri).unwrap();
            let options_doc = document_from_client_options(options);
            assert_eq!(options_doc, json_options)
        } else {
            let expected_type = if test_case.warning {
                "warning"
            } else {
                "error"
            };
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
