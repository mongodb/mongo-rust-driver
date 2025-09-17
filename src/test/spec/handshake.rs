use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    bson::{doc, oid::ObjectId, Bson, Document},
    cmap::Command,
    event::EventHandler,
    options::ClientOptions,
    test::{spec::unified_runner::run_unified_tests, topology_is_sharded},
};

use crate::{
    cmap::{
        conn::PendingConnection,
        establish::{ConnectionEstablisher, EstablisherOptions},
    },
    event::cmap::CmapEventEmitter,
    options::DriverInfo,
    test::get_client_options,
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
        event_emitter: CmapEventEmitter::new(None, ObjectId::new()),
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
    fn new(name: &str, version: Option<&str>, platform: Option<&str>) -> Self {
        Self {
            name: name.to_owned(),
            version: version.map(|v| v.to_owned()),
            platform: platform.map(|v| v.to_owned()),
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
    assert_eq!(metadata_a, metadata_b);
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
        DriverInfo::new("framework", Some("2.0"), Some("Framework Platform")),
        DriverInfo::new("framework", Some("2.0"), None),
        DriverInfo::new("framework", None, Some("Framework Platform")),
        DriverInfo::new("framework", None, None),
    ];
    for addl_info in test_info {
        let mut options = get_client_options().await.clone();
        options.max_idle_time = Some(Duration::from_millis(1));
        options.driver_info = Some(DriverInfo::new(
            "library",
            Some("1.2"),
            Some("Library Platform"),
        ));
        let hello = watch_hello(&mut options);
        let client = Client::with_options(options).unwrap();

        client.ping().await;
        let initial_client_metadata = hello.lock().unwrap().client_metadata();
        let (initial_name, initial_version, initial_platform) =
            extract_driver_info(&initial_client_metadata);
        tokio::time::sleep(Duration::from_millis(5)).await;

        client.append_metadata(addl_info.clone());
        client.ping().await;
        let test_client_metadata = hello.lock().unwrap().client_metadata();
        let (test_name, test_version, test_platform) = extract_driver_info(&test_client_metadata);

        // Compare updated metadata
        assert_eq!(test_name, format!("{initial_name}|{}", addl_info.name));
        if let Some(addl_version) = &addl_info.version {
            assert_eq!(test_version, format!("{initial_version}|{addl_version}"));
        } else {
            assert_eq!(test_version, initial_version);
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
        DriverInfo::new("framework", Some("2.0"), Some("Framework Platform")),
        DriverInfo::new("framework", Some("2.0"), None),
        DriverInfo::new("framework", None, Some("Framework Platform")),
        DriverInfo::new("framework", None, None),
    ];
    for addl_info in test_info {
        let mut options = get_client_options().await.clone();
        options.max_idle_time = Some(Duration::from_millis(1));
        let hello = watch_hello(&mut options);
        let client = Client::with_options(options).unwrap();

        client.append_metadata(DriverInfo::new(
            "library",
            Some("1.2"),
            Some("Library Platform"),
        ));
        client.ping().await;
        let initial_client_metadata = hello.lock().unwrap().client_metadata();
        let (initial_name, initial_version, initial_platform) =
            extract_driver_info(&initial_client_metadata);
        tokio::time::sleep(Duration::from_millis(5)).await;

        client.append_metadata(addl_info.clone());
        client.ping().await;
        let test_client_metadata = hello.lock().unwrap().client_metadata();
        let (test_name, test_version, test_platform) = extract_driver_info(&test_client_metadata);

        // Compare updated metadata
        assert_eq!(test_name, format!("{initial_name}|{}", addl_info.name));
        if let Some(addl_version) = &addl_info.version {
            assert_eq!(test_version, format!("{initial_version}|{addl_version}"));
        } else {
            assert_eq!(test_version, initial_version);
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
        DriverInfo::new("library", Some("1.2"), Some("Library Platform")),
        DriverInfo::new("framework", Some("1.2"), Some("Library Platform")),
        DriverInfo::new("library", Some("2.0"), Some("Library Platform")),
        DriverInfo::new("library", Some("1.2"), Some("Framework Platform")),
        DriverInfo::new("framework", Some("2.0"), Some("Library Platform")),
        DriverInfo::new("framework", Some("1.2"), Some("Framework Platform")),
        DriverInfo::new("library", Some("2.0"), Some("Framework Platform")),
    ];
    for info in test_info {
        let mut options = get_client_options().await.clone();
        options.max_idle_time = Some(Duration::from_millis(1));
        let hello = watch_hello(&mut options);
        let client = Client::with_options(options).unwrap();

        let setup_info = DriverInfo::new("library", Some("1.2"), Some("Library Platform"));
        client.append_metadata(setup_info.clone());
        client.ping().await;
        let updated_client_metadata = hello.lock().unwrap().client_metadata();
        tokio::time::sleep(Duration::from_millis(5)).await;

        client.append_metadata(info.clone());
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

    client.append_metadata(DriverInfo::new(
        "library",
        Some("1.2"),
        Some("Library Platform"),
    ));
    client.ping().await;
    tokio::time::sleep(Duration::from_millis(5)).await;

    client.append_metadata(DriverInfo::new(
        "framework",
        Some("2.0"),
        Some("Framework Platform"),
    ));
    client.ping().await;
    let client_metadata = hello.lock().unwrap().client_metadata();
    tokio::time::sleep(Duration::from_millis(5)).await;

    client.append_metadata(DriverInfo::new(
        "library",
        Some("1.2"),
        Some("Library Platform"),
    ));
    client.ping().await;
    let updated_client_metadata = hello.lock().unwrap().client_metadata();

    assert_eq!(client_metadata, updated_client_metadata);
}

// Client Metadata Update Prose Test 5: Metadata is not appended if identical to initial metadata
#[tokio::test]
async fn append_metadata_duplicate_of_initial() {
    let mut options = get_client_options().await.clone();
    options.max_idle_time = Some(Duration::from_millis(1));
    options.driver_info = Some(DriverInfo::new(
        "library",
        Some("1.2"),
        Some("Library Platform"),
    ));
    let hello = watch_hello(&mut options);
    let client = Client::with_options(options).unwrap();

    client.ping().await;
    let client_metadata = hello.lock().unwrap().client_metadata();
    tokio::time::sleep(Duration::from_millis(5)).await;

    client.append_metadata(DriverInfo::new(
        "library",
        Some("1.2"),
        Some("Library Platform"),
    ));
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
    options.driver_info = Some(DriverInfo::new(
        "library",
        Some("1.2"),
        Some("Library Platform"),
    ));
    let hello = watch_hello(&mut options);
    let client = Client::with_options(options).unwrap();

    client.ping().await;
    tokio::time::sleep(Duration::from_millis(5)).await;

    client.append_metadata(DriverInfo::new(
        "framework",
        Some("2.0"),
        Some("Framework Platform"),
    ));
    client.ping().await;
    let client_metadata = hello.lock().unwrap().client_metadata();
    tokio::time::sleep(Duration::from_millis(5)).await;

    client.append_metadata(DriverInfo::new(
        "library",
        Some("1.2"),
        Some("Library Platform"),
    ));
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
            DriverInfo::new("library", None, Some("Library Platform")),
            DriverInfo::new("library", Some(""), Some("Library Platform")),
        ),
        (
            DriverInfo::new("library", Some("1.2"), None),
            DriverInfo::new("library", Some("1.2"), Some("")),
        ),
    ];
    for (initial_info, appended_info) in test_info {
        let mut options = get_client_options().await.clone();
        options.max_idle_time = Some(Duration::from_millis(1));
        let hello = watch_hello(&mut options);
        let client = Client::with_options(options).unwrap();

        client.append_metadata(initial_info);
        client.ping().await;
        let initial_client_metadata = hello.lock().unwrap().client_metadata();
        tokio::time::sleep(Duration::from_millis(5)).await;

        client.append_metadata(appended_info);
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
            DriverInfo::new("library", None, Some("Library Platform")),
            DriverInfo::new("library", Some(""), Some("Library Platform")),
        ),
        (
            DriverInfo::new("library", Some("1.2"), None),
            DriverInfo::new("library", Some("1.2"), Some("")),
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

        client.append_metadata(appended_info);
        client.ping().await;
        let updated_client_metadata = hello.lock().unwrap().client_metadata();

        assert_eq!(initial_client_metadata, updated_client_metadata);
    }
}
