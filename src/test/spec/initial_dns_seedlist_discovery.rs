use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::{
    bson::doc,
    client::Client,
    options::{ClientOptions, ResolverConfig},
    test::{get_client_options, log_uncaptured, run_spec_test, TestClient},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestFile {
    uri: String,
    seeds: Option<Vec<String>>,
    hosts: Option<Vec<String>>,
    options: Option<ResolvedOptions>,
    parsed_options: Option<ParsedOptions>,
    error: Option<bool>,
    comment: Option<String>,
    #[serde(rename = "numSeeds")]
    num_seeds: Option<usize>,
    #[serde(rename = "numHosts")]
    num_hosts: Option<usize>,
    ping: Option<bool>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ResolvedOptions {
    replica_set: Option<String>,
    auth_source: Option<String>,
    ssl: bool,
    load_balanced: Option<bool>,
    direct_connection: Option<bool>,
    srv_max_hosts: Option<u32>,
    srv_service_name: Option<String>,
}

impl ResolvedOptions {
    fn assert_eq(&self, options: &ClientOptions) {
        // When an `authSource` is provided without any other authentication information, we do
        // not keep track of it within a Credential. The options present in the spec tests
        // expect the `authSource` be present regardless of whether a Credential should be
        // created, so the value of the `authSource` is not asserted on to avoid this
        // discrepancy.
        assert_eq!(self.replica_set, options.repl_set_name);
        assert_eq!(self.ssl, options.tls_options().is_some());
        assert_eq!(self.load_balanced, options.load_balanced);
        assert_eq!(self.direct_connection, options.direct_connection);
    }
}

#[derive(Debug, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
struct ParsedOptions {
    user: Option<String>,
    password: Option<String>,
    db: Option<String>,
}

async fn run_test(mut test_file: TestFile) {
    if let Some(ref options) = test_file.options {
        // TODO RUST-933: Remove this skip.
        let skip = if options.srv_service_name.is_some() {
            Some("srvServiceName")
        } else {
            None
        };

        if let Some(skip) = skip {
            log_uncaptured(format!(
                "skipping initial_dns_seedlist_discovery test case due to unsupported connection \
                 string option: {}",
                skip,
            ));
            return;
        }
    }

    // "encoded-userinfo-and-db.json" specifies a database name with a question mark which is
    // disallowed on Windows. See
    // <https://www.mongodb.com/docs/manual/reference/limits/#restrictions-on-db-names>
    if let Some(ref mut options) = test_file.parsed_options {
        if options.db.as_deref() == Some("mydb?") && cfg!(target_os = "windows") {
            options.db = Some("mydb".to_string());
            test_file.uri = test_file.uri.replace("%3F", "");
        }
    }

    let result = if cfg!(target_os = "windows") {
        ClientOptions::parse(&test_file.uri)
            .resolver_config(ResolverConfig::cloudflare())
            .await
    } else {
        ClientOptions::parse(&test_file.uri).await
    };

    if let Some(true) = test_file.error {
        assert!(result.is_err(), "{}", test_file.comment.unwrap());
        return;
    }

    assert!(result.is_ok(), "non-Ok result: {:?}", result);

    let options = result.unwrap();

    if let Some(ref mut expected_seeds) = test_file.seeds {
        let mut actual_seeds = options
            .hosts
            .iter()
            .map(|address| address.to_string())
            .collect::<Vec<_>>();

        expected_seeds.sort();
        actual_seeds.sort();

        assert_eq!(*expected_seeds, actual_seeds);
        if let Some(expected_seed_count) = test_file.num_seeds {
            assert_eq!(actual_seeds.len(), expected_seed_count)
        }
    }

    // "txt-record-with-overridden-ssl-option.json" requires SSL be disabled; see DRIVERS-1324.
    let requires_tls = match test_file.options {
        Some(ref options) => options.ssl,
        None => true,
    };
    let client = TestClient::new().await;
    if requires_tls != client.options().tls_options().is_some() {
        log_uncaptured(
            "skipping initial_dns_seedlist_discovery test case due to TLS requirement mismatch",
        )
    } else {
        let mut options_with_tls = options.clone();
        if requires_tls {
            options_with_tls
                .tls
                .clone_from(&get_client_options().await.tls);
        }

        let client = Client::with_options(options_with_tls).unwrap();
        if test_file.ping == Some(true) {
            client
                .database("db")
                .run_command(doc! { "ping" : 1 })
                .await
                .unwrap();
        }

        if let Some(ref mut hosts) = test_file.hosts {
            hosts.sort();
        }

        // This loop allows for some time to allow SDAM to discover the desired topology
        // TODO: RUST-232 or RUST-585: use SDAM monitoring / channels / timeouts to improve
        // this.
        let start = Instant::now();
        loop {
            let mut actual_hosts = client.get_hosts();
            actual_hosts.sort();

            if let Some(ref expected_hosts) = test_file.hosts {
                if actual_hosts == *expected_hosts {
                    break;
                }
            } else if let Some(expected_host_count) = test_file.num_hosts {
                if actual_hosts.len() == expected_host_count {
                    break;
                }
            } else if start.elapsed() > Duration::from_secs(5) {
                panic!(
                    "expected to eventually discover {:?}, instead found {:?}",
                    test_file.hosts, actual_hosts
                )
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    if let Some(ref mut resolved_options) = test_file.options {
        resolved_options.assert_eq(&options);
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

#[tokio::test]
async fn replica_set() {
    let client = TestClient::new().await;
    let skip =
        if client.is_replica_set() && client.options().repl_set_name.as_deref() != Some("repl0") {
            Some("repl_set_name != repl0")
        } else if !client.is_replica_set() {
            Some("not a replica set")
        } else {
            None
        };
    if let Some(skip) = skip {
        log_uncaptured(format!(
            "skipping initial_dns_seedlist_discovery::replica_set due to unmet topology \
             requirement ({})",
            skip
        ));
        return;
    }

    run_spec_test(&["initial-dns-seedlist-discovery", "replica-set"], run_test).await;
}

#[tokio::test]
async fn load_balanced() {
    let client = TestClient::new().await;
    if !client.is_load_balanced() {
        log_uncaptured(
            "skipping initial_dns_seedlist_discovery::load_balanced due to unmet topology \
             requirement (not a load balanced cluster)",
        );
        return;
    }
    run_spec_test(
        &["initial-dns-seedlist-discovery", "load-balanced"],
        run_test,
    )
    .await;
}

#[tokio::test]
async fn sharded() {
    let client = TestClient::new().await;
    if !client.is_sharded() {
        log_uncaptured(
            "skipping initial_dns_seedlist_discovery::sharded due to unmet topology requirement \
             (not a sharded cluster)",
        );
        return;
    }
    run_spec_test(&["initial-dns-seedlist-discovery", "sharded"], run_test).await;
}
