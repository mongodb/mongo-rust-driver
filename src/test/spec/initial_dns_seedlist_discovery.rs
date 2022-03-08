use std::time::{Duration, Instant};

use serde::Deserialize;
use tokio::sync::RwLockReadGuard;

use crate::{
    bson::doc,
    client::{auth::AuthMechanism, Client},
    options::{ClientOptions, ResolverConfig},
    runtime,
    test::{log_uncaptured, run_spec_test, TestClient, CLIENT_OPTIONS, LOCK},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ResolvedOptions {
    replica_set: Option<String>,
    auth_source: Option<String>,
    ssl: bool,
    load_balanced: Option<bool>,
    direct_connection: Option<bool>,
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
    // TODO DRIVERS-796: unskip this test
    if test_file.uri == "mongodb+srv://test5.test.build.10gen.cc/?authSource=otherDB" {
        log_uncaptured(
            "skipping initial_dns_seedlist_discovery due to authSource being specified without \
             credentials",
        );
        return;
    }

    // TODO RUST-980 unskip these tests
    if test_file
        .options
        .as_ref()
        .and_then(|o| o.load_balanced)
        .unwrap_or(false)
    {
        log_uncaptured(
            "skipping initial_dns_seedlist_discovery due to load-balanced test configuration",
        );
        return;
    }

    // "encoded-userinfo-and-db.json" specifies a database name with a question mark which is
    // disallowed on Windows. See
    // <https://docs.mongodb.com/manual/reference/limits/#restrictions-on-db-names>
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

    assert!(matches!(result, Ok(_)), "non-Ok result: {:?}", result);

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
    let skip = if !client.is_replica_set() {
        Some("not replica set")
    } else if client.options.repl_set_name.as_deref() != Some("repl0") {
        Some("repl_set_name != repl0")
    } else if requires_tls != client.options.tls_options().is_some() {
        Some("tls requirement mismatch")
    } else {
        None
    };
    if let Some(skip) = skip {
        log_uncaptured(format!(
            "skipping initial_dns_seedlist_discovery due to topology ({})",
            skip
        ))
    } else {
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
            options_with_tls.tls = CLIENT_OPTIONS.tls.clone();
        }

        let client = Client::with_options(options_with_tls).unwrap();
        client
            .database("db")
            .run_command(doc! { "ping" : 1 }, None)
            .await
            .unwrap();

        test_file.hosts.sort();

        // This loop allows for some time to allow SDAM to discover the desired topology
        // TODO: RUST-232 or RUST-585: use SDAM monitoring / channels / timeouts to improve
        // this.
        let start = Instant::now();
        loop {
            let mut actual_hosts = client.get_hosts().await;
            actual_hosts.sort();

            if actual_hosts == test_file.hosts {
                break;
            } else if start.elapsed() > Duration::from_secs(5) {
                panic!(
                    "expected to eventually discover {:?}, instead found {:?}",
                    test_file.hosts, actual_hosts
                )
            }

            runtime::delay_for(Duration::from_millis(500)).await;
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn replica_set() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;
    run_spec_test(&["initial-dns-seedlist-discovery", "replica-set"], run_test).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn load_balanced() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;
    run_spec_test(
        &["initial-dns-seedlist-discovery", "load-balanced"],
        run_test,
    )
    .await;
}
