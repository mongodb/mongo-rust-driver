use assert_matches::assert_matches;
use serde::Deserialize;

use crate::{options::ClientOptions, test::run_spec_test};

#[derive(Debug, Deserialize)]
struct TestFile {
    uri: String,
    seeds: Vec<String>,
    hosts: Vec<String>,
    options: Option<ResolvedOptions>,
    parsed_options: Option<ParsedOptions>,
    error: Option<bool>,
    comment: Option<String>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct ResolvedOptions {
    replica_set: Option<String>,
    auth_source: Option<String>,
    ssl: bool,
}

#[derive(Debug, Deserialize, Default, PartialEq)]
struct ParsedOptions {
    user: Option<String>,
    password: Option<String>,
    db: Option<String>,
}

#[test]
fn run() {
    let run_test = |mut test_file: TestFile| {
        let result = ClientOptions::parse(&test_file.uri);

        if let Some(true) = test_file.error {
            assert_matches!(result, Err(_));
            return;
        }

        assert_matches!(result, Ok(_));

        let options = result.unwrap();

        let mut expected_seeds = test_file.seeds.split_off(0);
        let mut actual_seeds = options
            .hosts
            .iter()
            .map(|address| address.to_string())
            .collect::<Vec<_>>();

        expected_seeds.sort();
        actual_seeds.sort();

        assert_eq!(expected_seeds, actual_seeds,);

        // The spec tests use two fields to compare against for the authentication database. In the
        // `parsed_options` field, there is a `db` field which is populated with the database
        // between the `/` and the querystring of the URI, and in the `options` field, there is an
        // `authSource` field that is populated with whatever the driver should resolve `authSource`
        // to from both the URI and the TXT options. Because we don't keep track of where we found
        // the auth source in ClientOptions and the point of testing this behavior in this spec is
        // to ensure that the right database is chosen based on both the URI and TXT options, we
        // just determine what that should be and stick that in all of the options parsed from the
        // spec JSON.
        let resolved_db = test_file
            .options
            .as_ref()
            .and_then(|opts| opts.auth_source.clone())
            .or_else(|| {
                test_file
                    .parsed_options
                    .as_ref()
                    .and_then(|opts| opts.db.clone())
            });

        if let Some(ref mut resolved_options) = test_file.options {
            resolved_options.auth_source = resolved_db.clone();

            let actual_options = ResolvedOptions {
                replica_set: options.repl_set_name.clone(),
                auth_source: options
                    .credential
                    .as_ref()
                    .and_then(|cred| cred.source.clone()),
                ssl: options.tls_options().is_some(),
            };

            assert_eq!(&*resolved_options, &actual_options);
        }

        if let Some(mut parsed_options) = test_file.parsed_options {
            parsed_options.db = resolved_db.clone();

            let actual_options = options
                .credential
                .map(|cred| ParsedOptions {
                    user: cred.username,
                    password: cred.password,
                    // In some spec tests, neither the `authSource` or `db` field are given, but in
                    // order to pass all the auth and URI options tests, the driver populates the
                    // credential's `source` field with "admin". To make it easier to assert here,
                    // we only populate the makeshift options with the credential's source if the
                    // JSON also specifies one of the database fields.
                    db: parsed_options.db.as_ref().and(cred.source),
                })
                .unwrap_or_default();

            assert_eq!(parsed_options, actual_options);
        }
    };

    run_spec_test(&["initial-dns-seedlist-discovery"], run_test);
}
