use std::str::FromStr;

use bson::Document;
use serde::Deserialize;

use crate::{
    options::{
        auth::{AuthMechanism, Credential},
        ClientOptions,
    },
    test::run_spec_test,
};

#[derive(Debug, Deserialize)]
struct TestFile {
    pub tests: Vec<TestCase>,
}

#[derive(Debug, Deserialize)]
struct TestCredential {
    pub username: Option<String>,
    pub password: Option<String>,
    pub source: Option<String>,
    pub mechanism: Option<String>,
    pub mechanism_properties: Option<Document>,
}

impl Into<Credential> for TestCredential {
    fn into(self) -> Credential {
        Credential {
            username: self.username,
            password: self.password,
            source: self.source,
            mechanism: self
                .mechanism
                .and_then(|s| AuthMechanism::from_str(s.as_str()).ok()),
            mechanism_properties: self.mechanism_properties,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TestCase {
    pub description: String,
    pub uri: String,
    pub valid: bool,
    pub credential: Option<TestCredential>,
}

/// Tests connection string parsing of authentication options.
fn run_auth_test(test_file: TestFile) {
    for mut test_case in test_file.tests {
        test_case.description = test_case.description.replace('$', "%");

        let skipped_mechanisms = ["GSSAPI", "MONGODB-X509", "PLAIN", "MONGODB-CR"];

        // TODO: X509 (RUST-147)
        // TODO: GSSAPI (RUST-196)
        // TODO: PLAIN (RUST-197)
        if skipped_mechanisms
            .iter()
            .any(|mech| test_case.description.contains(mech))
        {
            continue;
        }

        match ClientOptions::parse(test_case.uri.as_str()) {
            Ok(options) => {
                assert!(test_case.valid, "{}", test_case.description);
                match test_case.credential {
                    Some(credential) => {
                        assert!(options.credential.is_some(), "{}", test_case.description);
                        assert_eq!(
                            options.credential.unwrap(),
                            credential.into(),
                            "{}",
                            test_case.description
                        );
                    }
                    None => assert!(options.credential.is_none(), "{}", test_case.description),
                }
            }
            Err(_) => assert!(!test_case.valid, "{}", test_case.description),
        };
    }
}

#[test]
fn run() {
    run_spec_test(&["auth"], run_auth_test);
}
