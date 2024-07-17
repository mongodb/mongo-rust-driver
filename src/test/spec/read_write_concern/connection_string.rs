use serde::Deserialize;

use crate::{bson::Document, options::ClientOptions, test::run_spec_test};

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
    pub read_concern: Option<Document>,
    pub write_concern: Option<Document>,
}

fn normalize_write_concern_doc(mut write_concern_doc: Document) -> Document {
    if let Some(w_timeout) = write_concern_doc.remove("wtimeout") {
        write_concern_doc.insert("wtimeoutMS", w_timeout);
    }

    if let Some(j) = write_concern_doc.remove("j") {
        write_concern_doc.insert("journal", j);
    }

    write_concern_doc
}

async fn run_connection_string_test(test_file: TestFile) {
    for test_case in test_file.tests {
        match ClientOptions::parse(&test_case.uri).await {
            Ok(options) => {
                assert!(test_case.valid);

                if let Some(ref expected_read_concern) = test_case.read_concern {
                    let mut actual_read_concern = Document::new();

                    if let Some(client_read_concern) = options.read_concern {
                        actual_read_concern.insert("level", client_read_concern.level.as_str());
                    }

                    assert_eq!(
                        &actual_read_concern, expected_read_concern,
                        "{}",
                        test_case.description
                    );
                }

                if let Some(ref write_concern) = test_case.write_concern {
                    assert_eq!(
                        &normalize_write_concern_doc(
                            options
                                .write_concern
                                .map(|w| bson::to_document(&w).expect(&test_case.description))
                                .unwrap_or_default()
                        ),
                        write_concern,
                        "{}",
                        test_case.description
                    );
                }
            }
            Err(_) => {
                assert!(!test_case.valid, "{}", test_case.description);
            }
        };
    }
}

#[tokio::test]
async fn run() {
    run_spec_test(
        &["read-write-concern", "connection-string"],
        run_connection_string_test,
    )
    .await;
}
