use std::time::Duration;

use serde::Deserialize;

use crate::{
    bson::{Bson, Document},
    error::Error,
    options::{Acknowledgment, WriteConcern},
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
    pub valid: bool,
    pub write_concern: Option<Document>,
    pub write_concern_document: Option<Document>,
    pub read_concern: Option<Document>,
    pub read_concern_document: Option<Document>,
    pub is_acknowledged: Option<bool>,
}

fn write_concern_from_document(write_concern_doc: Document) -> Option<WriteConcern> {
    let mut write_concern = WriteConcern::default();

    for (key, value) in write_concern_doc {
        match (&key[..], value) {
            ("w", Bson::Int32(i)) => {
                write_concern.w = Some(Acknowledgment::from(i));
            }
            ("w", Bson::String(s)) => {
                write_concern.w = Some(Acknowledgment::from(s));
            }
            ("journal", Bson::Boolean(b)) => {
                write_concern.journal = Some(b);
            }
            ("wtimeoutMS", Bson::Int32(i)) if i > 0 => {
                write_concern.w_timeout = Some(Duration::from_millis(i as u64));
            }
            ("wtimeoutMS", Bson::Int32(_)) => {
                // WriteConcern has an unsigned integer for the wtimeout field, so this is
                // impossible to test.
                return None;
            }
            _ => {}
        };
    }

    Some(write_concern)
}

async fn run_document_test(test_file: TestFile) {
    for test_case in test_file.tests {
        if let Some(specified_write_concern) = test_case.write_concern {
            let wc = write_concern_from_document(specified_write_concern).map(|write_concern| {
                write_concern.validate().map_err(Error::from).and_then(|_| {
                    let doc = bson::to_bson(&write_concern)?;

                    Ok(doc)
                })
            });

            let actual_write_concern = match wc {
                Some(Ok(Bson::Document(write_concern))) => {
                    assert!(test_case.valid, "{}", &test_case.description);
                    write_concern
                }
                Some(Ok(x)) => panic!("wat: {:?}", x),
                Some(Err(_)) => {
                    assert!(!test_case.valid, "{}", &test_case.description);
                    continue;
                }
                None => {
                    continue;
                }
            };

            if let Some(expected_write_concern) = test_case.write_concern_document {
                assert_eq!(
                    actual_write_concern, expected_write_concern,
                    "{}",
                    &test_case.description
                );
            }
        }
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    run_spec_test(&["read-write-concern", "document"], run_document_test).await;
}
