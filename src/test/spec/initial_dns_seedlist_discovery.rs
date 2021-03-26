use serde::Deserialize;

use crate::{
    bson::doc,
    client::{auth::AuthMechanism, Client},
    options::{ClientOptions, ResolverConfig, Tls, TlsOptions},
    test::{run_spec_test, TestClient},
};

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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run() {
    async fn run_test(mut test_file: TestFile) {
        // TODO DRIVERS-796: unskip this test
        if test_file.uri == "mongodb+srv://test5.test.build.10gen.cc/?authSource=otherDB" {
            return;
        }

        // "encoded-userinfo-and-db.json" specifies a database name with a question mark which is
        // disallowed on Windows. See
        // https://docs.mongodb.com/manual/reference/limits/#restrictions-on-db-names
        if let Some(ref mut options) = test_file.parsed_options {
            if options.db.as_deref() == Some("mydb?") && cfg!(target_os = "windows") {
                options.db = Some("mydb".to_string());
                test_file.uri = test_file.uri.replace("%3F", "");
            }
        }

        let result = if cfg!(target_os = "windows") {
            ClientOptions::parse_with_resolver_config(&test_file.uri, ResolverConfig::cloudflare())
                .await
        } else {
            ClientOptions::parse(&test_file.uri).await
        };

        if let Some(true) = test_file.error {
            assert!(matches!(result, Err(_)), "{}", test_file.comment.unwrap());
            return;
        }

        assert!(matches!(result, Ok(_)));

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

        // "txt-record-with-overridden-ssl-option.json" requires SSL be disabled; see DRIVERS-1324.
        let requires_tls = match test_file.options {
            Some(ref options) => options.ssl,
            None => true,
        };
        let client = TestClient::new().await;
        // TODO RUST-395: log this test skip
        if requires_tls == client.options.tls_options().is_some()
            && client.is_replica_set()
            && client.options.repl_set_name.as_deref() == Some("repl0")
        {
            // If the connection URI provides authentication information, manually create the user
            // before connecting.
            if let Some(ParsedOptions {
                user: Some(ref user),
                password: Some(ref pwd),
                ref db,
            }) = test_file.parsed_options
            {
                client
                    .drop_and_create_user(
                        user,
                        pwd.as_str(),
                        &[],
                        &[AuthMechanism::ScramSha1, AuthMechanism::ScramSha256],
                        db.as_deref(),
                    )
                    .await
                    .unwrap();
            }

            let mut options_with_tls = options.clone();
            if requires_tls {
                let tls_options = TlsOptions::builder()
                    .allow_invalid_certificates(true)
                    .build();
                options_with_tls.tls = Some(Tls::Enabled(tls_options));
            }

            let client = Client::with_options(options_with_tls).unwrap();
            client
                .database("db")
                .run_command(doc! { "ping" : 1 }, None)
                .await
                .unwrap();
            let mut actual_hosts = client.get_hosts().await;

            test_file.hosts.sort();
            actual_hosts.sort();

            assert_eq!(test_file.hosts, actual_hosts);
        }

        if let Some(ref mut resolved_options) = test_file.options {
            // When an `authSource` is provided without any other authentication information, we do
            // not keep track of it within a Credential. The options present in the spec tests
            // expect the `authSource` be present regardless of whether a Credential should be
            // created, so the value of the `authSource` is not asserted on to avoid this
            // discrepancy.
            assert_eq!(resolved_options.replica_set, options.repl_set_name);
            assert_eq!(resolved_options.ssl, options.tls_options().is_some());
        }

        if let Some(parsed_options) = test_file.parsed_options {
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
    }

    run_spec_test(&["initial-dns-seedlist-discovery"], run_test).await;
}
