use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    bson::{doc, Bson, Document},
    cmap::{
        conn::PendingConnection,
        establish::{handshake::BASE_CLIENT_METADATA, ConnectionEstablisher, EstablisherOptions},
        Command,
    },
    event::{cmap::CmapEventEmitter, EventHandler},
    options::{ClientOptions, DriverInfo},
    test::{get_client_options, spec::unified_runner::run_unified_tests, topology_is_sharded},
    Client,
};

#[tokio::test]
async fn run_unified() {
    let mut runner = run_unified_tests(&["mongodb-handshake", "unified"]);
    if topology_is_sharded().await {
        // This test is flaky on sharded deployments.
        runner = runner.skip_tests(
            &[
                "metadata append does not create new connections or close existing ones and no \
                 hello command is sent",
            ],
        );
    }
    runner.await;
}

// Prose test 1: Test that the driver accepts an arbitrary auth mechanism
#[tokio::test]
async fn arbitrary_auth_mechanism() {
    let client_options = get_client_options().await;
    let mut options = EstablisherOptions::from(client_options);
    options.test_patch_reply = Some(|reply| {
        reply
            .as_mut()
            .unwrap()
            .command_response
            .sasl_supported_mechs
            .get_or_insert_with(Vec::new)
            .push("ArBiTrArY!".to_string());
    });
    let establisher = ConnectionEstablisher::new(options).unwrap();
    let pending = PendingConnection {
        id: 0,
        address: client_options.hosts[0].clone(),
        generation: crate::cmap::PoolGeneration::normal(),
        #[cfg(feature = "tracing-unstable")]
        event_emitter: CmapEventEmitter::new(None, crate::bson::oid::ObjectId::new(), None),
        #[cfg(not(feature = "tracing-unstable"))]
        event_emitter: CmapEventEmitter::new(None),
        time_created: Instant::now(),
        cancellation_receiver: None,
    };
    establisher
        .establish_connection(pending, None)
        .await
        .unwrap();
}

fn watch_hello(options: &mut ClientOptions) -> Arc<Mutex<Command>> {
    let hello: Arc<Mutex<Command>> = Arc::new(Mutex::new(Command::default()));
    let cb_hello = hello.clone();
    options.test_options_mut().hello_cb = Some(EventHandler::callback(move |command: Command| {
        *cb_hello.lock().unwrap() = command;
    }));
    hello
}

impl Command {
    fn client_metadata(&self) -> Document {
        self.body
            .get_document("client")
            .unwrap()
            .try_into()
            .unwrap()
    }
}

impl Client {
    async fn ping(&self) {
        self.database("admin")
            .run_command(doc! { "ping": 1 })
            .await
            .unwrap();
    }
}

impl DriverInfo {
    fn new<'a>(
        name: &str,
        version: impl Into<Option<&'a str>>,
        platform: impl Into<Option<&'a str>>,
    ) -> Self {
        Self {
            name: name.to_owned(),
            version: version.into().map(|v| v.to_owned()),
            platform: platform.into().map(|v| v.to_owned()),
        }
    }
}

fn assert_other_fields_eq(metadata_a: &Document, metadata_b: &Document) {
    let mut metadata_a = metadata_a.clone();
    metadata_a.remove("driver");
    metadata_a.remove("platform");
    let mut metadata_b = metadata_b.clone();
    metadata_b.remove("driver");
    metadata_b.remove("platform");

    assert_eq!(metadata_a.len(), metadata_b.len());
    for (k, mut v) in metadata_a {
        // The "architecture" field in the "os" document may be removed during truncation.
        if k == "os" {
            let os_a = v.as_document_mut().unwrap();
            let os_b = metadata_b.get_document_mut("os").unwrap();
            if let (Some(architecture_a), Some(architecture_b)) =
                (os_a.remove("architecture"), os_b.remove("architecture"))
            {
                assert_eq!(architecture_a, architecture_b);
            }
            assert_eq!(os_a, os_b);
        } else {
            assert_eq!(&v, metadata_b.get(k).unwrap());
        }
    }
}

fn extract_driver_info(metadata: &Document) -> (&str, &str, &str) {
    (
        metadata["driver"]["name"].as_str().unwrap(),
        metadata["driver"]["version"].as_str().unwrap(),
        metadata["platform"].as_str().unwrap(),
    )
}

// Client Metadata Update Prose Test 1: Test that the driver updates metadata
#[tokio::test]
async fn append_metadata_driver_update() {
    let test_info = [
        DriverInfo::new("framework", "2.0", "Framework Platform"),
        DriverInfo::new("framework", "2.0", None),
        DriverInfo::new("framework", None, "Framework Platform"),
        DriverInfo::new("framework", None, None),
    ];
    for addl_info in test_info {
        let mut options = get_client_options().await.clone();
        options.max_idle_time = Some(Duration::from_millis(1));
        options.driver_info = Some(DriverInfo::new("library", "1.2", "Library Platform"));
        let hello = watch_hello(&mut options);
        let client = Client::with_options(options).unwrap();

        client.ping().await;
        let initial_client_metadata = hello.lock().unwrap().client_metadata();
        let (initial_name, initial_version, initial_platform) =
            extract_driver_info(&initial_client_metadata);
        tokio::time::sleep(Duration::from_millis(5)).await;

        client.append_metadata(addl_info.clone()).unwrap();
        client.ping().await;
        let test_client_metadata = hello.lock().unwrap().client_metadata();
        let (test_name, test_version, test_platform) = extract_driver_info(&test_client_metadata);

        // Compare updated metadata
        assert_eq!(test_name, format!("{initial_name}|{}", addl_info.name));
        if let Some(addl_version) = &addl_info.version {
            assert_eq!(test_version, format!("{initial_version}|{addl_version}"));
        } else {
            assert_eq!(test_version, format!("{initial_version}|"));
        }
        if let Some(addl_platform) = &addl_info.platform {
            assert_eq!(test_platform, format!("{initial_platform}|{addl_platform}"));
        } else {
            assert_eq!(test_platform, initial_platform);
        }

        assert_other_fields_eq(&initial_client_metadata, &test_client_metadata);
    }
}

// Client Metadata Update Prose Test 2: Multiple Successive Metadata Updates
#[tokio::test]
async fn append_metadata_successive_updates() {
    let test_info = [
        DriverInfo::new("framework", "2.0", "Framework Platform"),
        DriverInfo::new("framework", "2.0", None),
        DriverInfo::new("framework", None, "Framework Platform"),
        DriverInfo::new("framework", None, None),
    ];
    for addl_info in test_info {
        let mut options = get_client_options().await.clone();
        options.max_idle_time = Some(Duration::from_millis(1));
        let hello = watch_hello(&mut options);
        let client = Client::with_options(options).unwrap();

        client
            .append_metadata(DriverInfo::new("library", "1.2", "Library Platform"))
            .unwrap();
        client.ping().await;
        let initial_client_metadata = hello.lock().unwrap().client_metadata();
        let (initial_name, initial_version, initial_platform) =
            extract_driver_info(&initial_client_metadata);
        tokio::time::sleep(Duration::from_millis(5)).await;

        client.append_metadata(addl_info.clone()).unwrap();
        client.ping().await;
        let test_client_metadata = hello.lock().unwrap().client_metadata();
        let (test_name, test_version, test_platform) = extract_driver_info(&test_client_metadata);

        // Compare updated metadata
        assert_eq!(test_name, format!("{initial_name}|{}", addl_info.name));
        if let Some(addl_version) = &addl_info.version {
            assert_eq!(test_version, format!("{initial_version}|{addl_version}"));
        } else {
            assert_eq!(test_version, format!("{initial_version}|"));
        }
        if let Some(addl_platform) = &addl_info.platform {
            assert_eq!(test_platform, format!("{initial_platform}|{addl_platform}"));
        } else {
            assert_eq!(test_platform, initial_platform);
        }

        assert_other_fields_eq(&initial_client_metadata, &test_client_metadata);
    }
}

// Client Metadata Update Prose Test 3: Multiple Successive Metadata Updates with Duplicate Data
#[tokio::test]
async fn append_metadata_duplicate_successive() {
    let test_info = [
        DriverInfo::new("library", "1.2", "Library Platform"),
        DriverInfo::new("framework", "1.2", "Library Platform"),
        DriverInfo::new("library", "2.0", "Library Platform"),
        DriverInfo::new("library", "1.2", "Framework Platform"),
        DriverInfo::new("framework", "2.0", "Library Platform"),
        DriverInfo::new("framework", "1.2", "Framework Platform"),
        DriverInfo::new("library", "2.0", "Framework Platform"),
    ];
    for info in test_info {
        let mut options = get_client_options().await.clone();
        options.max_idle_time = Some(Duration::from_millis(1));
        let hello = watch_hello(&mut options);
        let client = Client::with_options(options).unwrap();

        let setup_info = DriverInfo::new("library", "1.2", "Library Platform");
        client.append_metadata(setup_info.clone()).unwrap();
        client.ping().await;
        let updated_client_metadata = hello.lock().unwrap().client_metadata();
        tokio::time::sleep(Duration::from_millis(5)).await;

        client.append_metadata(info.clone()).unwrap();
        client.ping().await;
        let test_client_metadata = hello.lock().unwrap().client_metadata();
        let strip_default = |value: &Bson| {
            value
                .as_str()
                .unwrap()
                .split('|')
                .skip(1)
                .collect::<Vec<_>>()
                .join("|")
        };
        let test_name = strip_default(&test_client_metadata["driver"]["name"]);
        let test_version = strip_default(&test_client_metadata["driver"]["version"]);
        let test_platform = strip_default(&test_client_metadata["platform"]);

        // Compare updated metadata
        if info == setup_info {
            assert_eq!(test_name, "library");
            assert_eq!(test_version, "1.2");
            assert_eq!(test_platform, "Library Platform");
        } else {
            assert_eq!(test_name, format!("library|{}", info.name));
            assert_eq!(test_version, format!("1.2|{}", info.spec_version()));
            assert_eq!(
                test_platform,
                format!("Library Platform|{}", info.spec_platform())
            );
        }

        assert_other_fields_eq(&updated_client_metadata, &test_client_metadata);
    }
}

// Client Metadata Update Prose Test 4: Multiple Metadata Updates with Duplicate Data
#[tokio::test]
async fn append_metadata_duplicate_multiple() {
    let mut options = get_client_options().await.clone();
    options.max_idle_time = Some(Duration::from_millis(1));
    let hello = watch_hello(&mut options);
    let client = Client::with_options(options).unwrap();

    client
        .append_metadata(DriverInfo::new("library", "1.2", "Library Platform"))
        .unwrap();
    client.ping().await;
    tokio::time::sleep(Duration::from_millis(5)).await;

    client
        .append_metadata(DriverInfo::new("framework", "2.0", "Framework Platform"))
        .unwrap();
    client.ping().await;
    let client_metadata = hello.lock().unwrap().client_metadata();
    tokio::time::sleep(Duration::from_millis(5)).await;

    client
        .append_metadata(DriverInfo::new("library", "1.2", "Library Platform"))
        .unwrap();
    client.ping().await;
    let updated_client_metadata = hello.lock().unwrap().client_metadata();

    assert_eq!(client_metadata, updated_client_metadata);
}

// Client Metadata Update Prose Test 5: Metadata is not appended if identical to initial metadata
#[tokio::test]
async fn append_metadata_duplicate_of_initial() {
    let mut options = get_client_options().await.clone();
    options.max_idle_time = Some(Duration::from_millis(1));
    options.driver_info = Some(DriverInfo::new("library", "1.2", "Library Platform"));
    let hello = watch_hello(&mut options);
    let client = Client::with_options(options).unwrap();

    client.ping().await;
    let client_metadata = hello.lock().unwrap().client_metadata();
    tokio::time::sleep(Duration::from_millis(5)).await;

    client
        .append_metadata(DriverInfo::new("library", "1.2", "Library Platform"))
        .unwrap();
    client.ping().await;
    let updated_client_metadata = hello.lock().unwrap().client_metadata();

    assert_eq!(client_metadata, updated_client_metadata);
}

// Client Metadata Update Prose Test 6: Metadata is not appended if identical to initial metadata
// (separated by non-identical metadata)
#[tokio::test]
async fn append_metadata_duplicate_of_initial_separated() {
    let mut options = get_client_options().await.clone();
    options.max_idle_time = Some(Duration::from_millis(1));
    options.driver_info = Some(DriverInfo::new("library", "1.2", "Library Platform"));
    let hello = watch_hello(&mut options);
    let client = Client::with_options(options).unwrap();

    client.ping().await;
    tokio::time::sleep(Duration::from_millis(5)).await;

    client
        .append_metadata(DriverInfo::new("framework", "2.0", "Framework Platform"))
        .unwrap();
    client.ping().await;
    let client_metadata = hello.lock().unwrap().client_metadata();
    tokio::time::sleep(Duration::from_millis(5)).await;

    client
        .append_metadata(DriverInfo::new("library", "1.2", "Library Platform"))
        .unwrap();
    client.ping().await;
    let updated_client_metadata = hello.lock().unwrap().client_metadata();

    assert_eq!(client_metadata, updated_client_metadata);
}

// Client Metadata Update Prose Test 7: Empty strings are considered unset when appending duplicate
// metadata
#[tokio::test]
async fn append_metadata_duplicate_empty_strings() {
    let test_info = [
        (
            DriverInfo::new("library", None, "Library Platform"),
            DriverInfo::new("library", "", "Library Platform"),
        ),
        (
            DriverInfo::new("library", "1.2", None),
            DriverInfo::new("library", "1.2", ""),
        ),
    ];
    for (initial_info, appended_info) in test_info {
        let mut options = get_client_options().await.clone();
        options.max_idle_time = Some(Duration::from_millis(1));
        let hello = watch_hello(&mut options);
        let client = Client::with_options(options).unwrap();

        client.append_metadata(initial_info).unwrap();
        client.ping().await;
        let initial_client_metadata = hello.lock().unwrap().client_metadata();
        tokio::time::sleep(Duration::from_millis(5)).await;

        client.append_metadata(appended_info).unwrap();
        client.ping().await;
        let updated_client_metadata = hello.lock().unwrap().client_metadata();

        assert_eq!(initial_client_metadata, updated_client_metadata);
    }
}

// Client Metadata Update Prose Test 8: Empty strings are considered unset when appending metadata
// identical to initial metadata
#[tokio::test]
async fn append_metadata_duplicate_empty_strings_initial() {
    let test_info = [
        (
            DriverInfo::new("library", None, "Library Platform"),
            DriverInfo::new("library", "", "Library Platform"),
        ),
        (
            DriverInfo::new("library", "1.2", None),
            DriverInfo::new("library", "1.2", ""),
        ),
    ];
    for (initial_info, appended_info) in test_info {
        let mut options = get_client_options().await.clone();
        options.max_idle_time = Some(Duration::from_millis(1));
        options.driver_info = Some(initial_info);
        let hello = watch_hello(&mut options);
        let client = Client::with_options(options).unwrap();

        client.ping().await;
        let initial_client_metadata = hello.lock().unwrap().client_metadata();
        tokio::time::sleep(Duration::from_millis(5)).await;

        client.append_metadata(appended_info).unwrap();
        client.ping().await;
        let updated_client_metadata = hello.lock().unwrap().client_metadata();

        assert_eq!(initial_client_metadata, updated_client_metadata);
    }
}

// Client Metadata Update Prose Test 10: Entries in driver.name and driver.version correspond by
// index
#[tokio::test]
async fn append_metadata_name_version_correspond() {
    let base = BASE_CLIENT_METADATA.clone();
    let driver_name = base.driver.name;
    let driver_version = base.driver.version;
    let test_info = [
        (
            "Gap in middle (name)",
            vec![
                DriverInfo::new("", None, None),
                DriverInfo::new("F2", None, None),
            ],
            format!("{driver_name}||F2"),
            format!("{driver_version}||"),
        ),
        (
            "Gap in middle (version)",
            vec![
                DriverInfo::new("F1", None, None),
                DriverInfo::new("F2", "2.0", None),
            ],
            format!("{driver_name}|F1|F2"),
            format!("{driver_version}||2.0"),
        ),
        (
            "Trailing delimiter retained",
            vec![DriverInfo::new("F1", None, None)],
            format!("{driver_name}|F1"),
            format!("{driver_version}|"),
        ),
        (
            "Equal versions do not collapse",
            vec![DriverInfo::new("F1", driver_version.as_str(), None)],
            format!("{driver_name}|F1"),
            format!("{driver_version}|{driver_version}"),
        ),
        (
            "Equal names do not collapse",
            vec![DriverInfo::new(driver_name.as_str(), "1.0", None)],
            format!("{driver_name}|{driver_name}"),
            format!("{driver_version}|1.0"),
        ),
        (
            "Duplicates deduplicate",
            vec![
                DriverInfo::new("F1", "1.0", None),
                DriverInfo::new("F1", "1.0", None),
            ],
            format!("{driver_name}|F1"),
            format!("{driver_version}|1.0"),
        ),
        (
            "All versions absent",
            vec![
                DriverInfo::new("F1", None, None),
                DriverInfo::new("F2", None, None),
            ],
            format!("{driver_name}|F1|F2"),
            format!("{driver_version}||"),
        ),
        (
            "All names absent",
            vec![
                DriverInfo::new("", "1.0", None),
                DriverInfo::new("", "2.0", None),
            ],
            format!("{driver_name}||"),
            format!("{driver_version}|1.0|2.0"),
        ),
        (
            "Non-adjacent duplicate",
            vec![
                DriverInfo::new("F1", "1.0", None),
                DriverInfo::new("F2", "2.0", None),
                DriverInfo::new("F1", "1.0", None),
            ],
            format!("{driver_name}|F1|F2"),
            format!("{driver_version}|1.0|2.0"),
        ),
        (
            "Platform-only difference is not a duplicate",
            vec![
                DriverInfo::new("F1", "1.0", "P1"),
                DriverInfo::new("F1", "1.0", "P2"),
            ],
            format!("{driver_name}|F1|F1"),
            format!("{driver_version}|1.0|1.0"),
        ),
        (
            "Wrapper matching the driver's own identity",
            vec![DriverInfo::new(
                driver_name.as_str(),
                driver_version.as_str(),
                None,
            )],
            format!("{driver_name}|{driver_name}"),
            format!("{driver_version}|{driver_version}"),
        ),
        (
            "Duplicates with an unset field deduplicate",
            vec![
                DriverInfo::new("F1", None, None),
                DriverInfo::new("F1", None, None),
            ],
            format!("{driver_name}|F1"),
            format!("{driver_version}|"),
        ),
    ];
    for (desc, infos, exp_name, exp_version) in test_info {
        // 1. Create a MongoClient instance with a maxIdleTimeMS set to 1ms
        let mut options = get_client_options().await.clone();
        options.max_idle_time = Some(Duration::from_millis(1));
        let hello = watch_hello(&mut options);
        let client = Client::with_options(options).unwrap();

        // append info
        for info in infos {
            client.append_metadata(info).unwrap();
        }

        // 2. Send a ping command to the server and verify that the command succeeds.
        client.ping().await;

        // 3. Wait 5ms for the connection to become idle.
        tokio::time::sleep(Duration::from_millis(5)).await;

        // validate expectations
        let metadata = hello.lock().unwrap().client_metadata();
        let actual_name = metadata["driver"]["name"].as_str().unwrap();
        let actual_version = metadata["driver"]["version"].as_str().unwrap();
        assert_eq!(
            exp_name, actual_name,
            "[{desc}]: expected name {exp_name:?}, got {actual_name:?}"
        );
        assert_eq!(
            exp_version, actual_version,
            "[{desc}]: expected version {exp_version:?}, got {actual_version:?}"
        );
    }
}

// Client Metadata Update Prose Test 11: Appending metadata containing the delimiter raises an error
#[tokio::test]
async fn metadata_delimiter_error() {
    let base = BASE_CLIENT_METADATA.clone();
    let driver_name = base.driver.name;
    let driver_version = base.driver.version;
    let driver_platform = base.platform;
    let test_info = [
        DriverInfo::new("frame|work", "2.0", "Framework Platform"),
        DriverInfo::new("framework", "2|0", "Framework Platform"),
        DriverInfo::new("framework", "2.0", "Framework|Platform"),
    ];
    for info in test_info {
        // 1. Create a MongoClient instance
        let mut options = get_client_options().await.clone();
        options.max_idle_time = Some(Duration::from_millis(1));
        options.driver_info = Some(DriverInfo::new("library", "1.2", "Library Platform"));
        let hello = watch_hello(&mut options);
        let client = Client::with_options(options).unwrap();

        // 2. Send a ping command to the server and verify that the command succeeds.
        client.ping().await;

        // 3. Wait 5ms for the connection to become idle.
        tokio::time::sleep(Duration::from_millis(5)).await;

        // 4. Append the DriverInfoOptions from the selected test case and assert that an error is
        //    raised.
        assert!(
            client.append_metadata(info.clone()).is_err(),
            "expected error from appending {info:?}"
        );

        // 5. Wait 5ms for the connection to become idle so that the next operation establishes a
        //    new connection and handshakes again.
        tokio::time::sleep(Duration::from_millis(5)).await;

        // 6. Assert that the intercepted client document is unchanged by the failed append
        let metadata = hello.lock().unwrap().client_metadata();
        assert_eq!(
            metadata["driver"]["name"],
            Bson::String(format!("{driver_name}|library"))
        );
        assert_eq!(
            metadata["driver"]["version"],
            Bson::String(format!("{driver_version}|1.2"))
        );
        assert_eq!(
            metadata["platform"],
            Bson::String(format!("{driver_platform}|Library Platform"))
        );
    }
}

#[tokio::test]
async fn handshake_includes_backpressure() {
    let mut options = get_client_options().await.clone();
    let hello = watch_hello(&mut options);
    let client = Client::for_test().options(options).await;
    client
        .database("db")
        .run_command(doc! { "ping": 1 })
        .await
        .unwrap();

    let command = hello.lock().unwrap();
    assert_eq!(command.body.get_str("backpressure").unwrap(), "2");
}
