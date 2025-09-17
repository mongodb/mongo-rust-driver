use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    bson::{doc, oid::ObjectId, Bson, Document},
    cmap::Command,
    event::EventHandler,
    options::ClientOptions,
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

enum InitialMetadata {
    InOptions,
    Appended,
}

impl InitialMetadata {
    async fn run_test(self) {
        let test_info = [
            DriverInfo {
                name: "framework".to_owned(),
                version: Some("2.0".to_owned()),
                platform: Some("Framework Platform".to_owned()),
            },
            DriverInfo {
                name: "framework".to_owned(),
                version: Some("2.0".to_owned()),
                platform: None,
            },
            DriverInfo {
                name: "framework".to_owned(),
                version: None,
                platform: Some("Framework Platform".to_owned()),
            },
            DriverInfo {
                name: "framework".to_owned(),
                version: None,
                platform: None,
            },
        ];
        for addl_info in test_info {
            let mut options = get_client_options().await.clone();
            options.max_idle_time = Some(Duration::from_millis(1));
            let initial_info = DriverInfo {
                name: "library".to_owned(),
                version: Some("1.2".to_owned()),
                platform: Some("Library Platform".to_owned()),
            };
            let to_append = match self {
                InitialMetadata::InOptions => {
                    options.driver_info = Some(initial_info);
                    None
                }
                InitialMetadata::Appended => Some(initial_info),
            };
            let hello = watch_hello(&mut options);
            let client = Client::with_options(options).unwrap();

            if let Some(append_info) = to_append {
                client.append_metadata(append_info);
            }
            client.ping().await;
            let initial_client_metadata = hello.lock().unwrap().client_metadata();
            let initial_name = initial_client_metadata["driver"]["name"].as_str().unwrap();
            let initial_version = initial_client_metadata["driver"]["version"]
                .as_str()
                .unwrap();
            let initial_platform = initial_client_metadata["platform"].as_str().unwrap();

            tokio::time::sleep(Duration::from_millis(5)).await;

            client.append_metadata(addl_info.clone());
            client.ping().await;
            let test_client_metadata = hello.lock().unwrap().client_metadata();
            let test_name = test_client_metadata["driver"]["name"].as_str().unwrap();
            let test_version = test_client_metadata["driver"]["version"].as_str().unwrap();
            let test_platform = test_client_metadata["platform"].as_str().unwrap();

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

            // Everything else should be the same
            let mut initial_client_metadata = initial_client_metadata;
            initial_client_metadata.remove("driver");
            initial_client_metadata.remove("platform");
            let mut test_client_metadata = test_client_metadata;
            test_client_metadata.remove("driver");
            test_client_metadata.remove("platform");
            assert_eq!(initial_client_metadata, test_client_metadata);
        }
    }
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

// Client Metadata Update Prose Test 1: Test that the driver updates metadata
#[tokio::test]
async fn append_metadata_driver_update() {
    InitialMetadata::InOptions.run_test().await
}

// Client Metadata Update Prose Test 2: Multiple Successive Metadata Updates
#[tokio::test]
async fn append_metadata_successive_updates() {
    InitialMetadata::Appended.run_test().await
}

// Client Metadata Update Prose Test 3: Multiple Successive Metadata Updates with Duplicate Data
#[tokio::test]
async fn append_metadata_duplicate_successive() {
    let test_info = [DriverInfo {
        name: "library".to_owned(),
        version: Some("1.2".to_owned()),
        platform: Some("Library Platform".to_owned()),
    }];
    for info in test_info {
        let mut options = get_client_options().await.clone();
        options.max_idle_time = Some(Duration::from_millis(1));
        let hello = watch_hello(&mut options);
        let client = Client::with_options(options).unwrap();

        let setup_info = DriverInfo {
            name: "library".to_owned(),
            version: Some("1.2".to_owned()),
            platform: Some("Library Platform".to_owned()),
        };
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
                test_version,
                format!("Library Platform|{}", info.spec_platform())
            );
        }

        // Everything else should be the same
        let mut updated_client_metadata = updated_client_metadata;
        updated_client_metadata.remove("driver");
        updated_client_metadata.remove("platform");
        let mut test_client_metadata = test_client_metadata;
        test_client_metadata.remove("driver");
        test_client_metadata.remove("platform");
        assert_eq!(updated_client_metadata, test_client_metadata);
    }
}
