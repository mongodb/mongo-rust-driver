use std::str::FromStr;

use serde::Deserialize;

use crate::{
    bson::Document,
    options::{AuthMechanism, ClientOptions, Credential},
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

impl From<TestCredential> for Credential {
    fn from(test_credential: TestCredential) -> Self {
        Self {
            username: test_credential.username,
            password: test_credential.password,
            source: test_credential.source,
            mechanism: test_credential
                .mechanism
                .and_then(|s| AuthMechanism::from_str(s.as_str()).ok()),
            mechanism_properties: test_credential.mechanism_properties,
            oidc_callback: crate::client::auth::oidc::Callback::new(),
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
async fn run_auth_test(test_file: TestFile) {
    for mut test_case in test_file.tests {
        test_case.description = test_case.description.replace('$', "%");

        let skipped_mechanisms = [
            "GSSAPI",
            "MONGODB-CR",
            #[cfg(not(feature = "aws-auth"))]
            "MONGODB-AWS",
        ];

        // TODO: GSSAPI (RUST-196)
        if skipped_mechanisms
            .iter()
            .any(|mech| test_case.description.contains(mech))
        {
            continue;
        }
        // This one's GSSAPI but doesn't include it in the description
        if test_case
            .description
            .contains("must raise an error when the hostname canonicalization is invalid")
        {
            continue;
        }
        // Lack of callback can't be validated from just URI parsing, as it's set in code
        if test_case
            .description
            .contains("should throw an exception if neither environment nor callbacks specified")
        {
            continue;
        }

        match ClientOptions::parse(test_case.uri.as_str()).await {
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
            Err(e) => assert!(
                !test_case.valid,
                "got error {:?}: {}",
                e, test_case.description
            ),
        };
    }
}

#[tokio::test]
async fn run_legacy() {
    run_spec_test(&["auth", "legacy"], run_auth_test).await;
}

// TODO RUST-1665: run unified tests
