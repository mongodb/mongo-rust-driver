use serde::Deserialize;

use crate::{
    bson::Document,
    error::Error,
    options::{ReadConcern, WriteConcern},
    test::run_spec_test,
};

#[derive(Debug, Deserialize)]
struct TestFile {
    pub tests: Vec<TestCase>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TestCase {
    pub description: String,
    pub valid: bool,
    pub write_concern: Option<Document>,
    pub write_concern_document: Option<Document>,
    pub read_concern: Option<Document>,
    pub read_concern_document: Option<Document>,
    pub is_server_default: Option<bool>,
    pub is_acknowledged: Option<bool>,
}

async fn run_document_test(test_file: TestFile) {
    for test_case in test_file.tests {
        let description = test_case.description.as_str();

        if let Some(specified_write_concern_document) = test_case.write_concern {
            let specified_write_concern =
                match crate::bson::from_document::<WriteConcern>(specified_write_concern_document)
                    .map_err(Error::from)
                    .and_then(|wc| wc.validate().map(|_| wc))
                {
                    Ok(write_concern) => {
                        assert!(
                            test_case.valid,
                            "Write concern deserialization/validation should fail: {}",
                            description
                        );
                        write_concern
                    }
                    Err(err) => {
                        assert!(
                            !test_case.valid,
                            "Write concern deserialization/validation should succeed but got \
                             {:?}: {}",
                            err, description,
                        );
                        continue;
                    }
                };

            if let Some(is_server_default) = test_case.is_server_default {
                assert_eq!(
                    specified_write_concern.is_empty(),
                    is_server_default,
                    "is_server_default is {} but write concern is {:?}: {}",
                    is_server_default,
                    &specified_write_concern,
                    description
                );
            }

            if let Some(is_acknowledged) = test_case.is_acknowledged {
                assert_eq!(
                    specified_write_concern.is_acknowledged(),
                    is_acknowledged,
                    "is_acknowledged is {} but write concern is {:?}: {}",
                    is_acknowledged,
                    &specified_write_concern,
                    description
                );
            }

            let actual_write_concern_document = crate::bson::to_document(&specified_write_concern)
                .unwrap_or_else(|err| {
                    panic!(
                        "Write concern serialization should succeed but got {:?}: {}",
                        err, description
                    )
                });

            if let Some(expected_write_concern_document) = test_case.write_concern_document {
                assert_eq!(
                    actual_write_concern_document, expected_write_concern_document,
                    "{}",
                    description
                );
            }
        }

        if let Some(specified_read_concern_document) = test_case.read_concern {
            // It's impossible to construct an empty `ReadConcern` (i.e. the server's default) in
            // the Rust driver so we skip this test.
            if test_case.description == "Default" {
                continue;
            }

            let specified_read_concern: ReadConcern =
                crate::bson::from_document(specified_read_concern_document).unwrap_or_else(|err| {
                    panic!(
                        "Read concern deserialization should succeed but got {:?}: {}",
                        err, description,
                    )
                });

            let actual_read_concern_document = crate::bson::to_document(&specified_read_concern)
                .unwrap_or_else(|err| {
                    panic!(
                        "Read concern serialization should succeed but got: {:?}: {}",
                        err, description
                    )
                });

            if let Some(expected_read_concern_document) = test_case.read_concern_document {
                assert_eq!(
                    actual_read_concern_document, expected_read_concern_document,
                    "{}",
                    description
                );
            }
        }
    }
}

#[tokio::test]
async fn run() {
    run_spec_test(&["read-write-concern", "document"], run_document_test).await;
}
