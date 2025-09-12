use std::time::{Duration, Instant};

use crate::{
    bson::{doc, oid::ObjectId, Bson, Document},
    cmap::Command,
    sdam::topology::TopologySpec,
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
    let mut options = EstablisherOptions::from(&TopologySpec::from(client_options.clone()));
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
            let (hs_sender, mut hellos) = tokio::sync::mpsc::channel::<Command>(100);
            let mut latest_hello = async move || {
                let mut out = hellos.recv().await.unwrap();
                while let Ok(hello) = hellos.try_recv() {
                    out = hello;
                }
                out
            };
            options.test_options_mut().hello_sender = Some(hs_sender);
            let client = Client::with_options(options).unwrap();
            if let Some(append_info) = to_append {
                client.append_metadata(append_info);
            }

            client
                .database("admin")
                .run_command(doc! { "ping": 1 })
                .await
                .unwrap();
            let initial_hello = latest_hello().await;
            let mut initial_client_metadata: Document = initial_hello
                .body
                .get_document("client")
                .unwrap()
                .try_into()
                .unwrap();

            tokio::time::sleep(Duration::from_millis(5)).await;

            client.append_metadata(addl_info.clone());
            client
                .database("admin")
                .run_command(doc! { "ping": 1 })
                .await
                .unwrap();
            let test_hello = latest_hello().await;
            let mut test_client_metadata: Document = test_hello
                .body
                .get_document("client")
                .unwrap()
                .try_into()
                .unwrap();

            // Compare updated metadata
            let expected_name = Bson::String(format!(
                "{}|{}",
                initial_client_metadata["driver"]["name"].as_str().unwrap(),
                addl_info.name
            ));
            assert_eq!(expected_name, test_client_metadata["driver"]["name"],);

            let initial_version = &initial_client_metadata["driver"]["version"];
            let expected_version = if let Some(version) = &addl_info.version {
                Bson::String(format!("{}|{}", initial_version.as_str().unwrap(), version))
            } else {
                initial_version.clone()
            };
            assert_eq!(expected_version, test_client_metadata["driver"]["version"]);

            let initial_platform = &initial_client_metadata["platform"];
            let expected_platform = if let Some(platform) = &addl_info.platform {
                Bson::String(format!(
                    "{}|{}",
                    initial_platform.as_str().unwrap(),
                    platform
                ))
            } else {
                initial_platform.clone()
            };
            assert_eq!(expected_platform, test_client_metadata["platform"]);

            // Everything else should be the same
            initial_client_metadata.remove("driver");
            initial_client_metadata.remove("platform");
            test_client_metadata.remove("driver");
            test_client_metadata.remove("platform");
            assert_eq!(initial_client_metadata, test_client_metadata);
        }
    }
}

// Client Metadata Update Prose Test 1: Test that the driver updates metadata
#[tokio::test]
async fn driver_updates_metadata() {
    InitialMetadata::InOptions.run_test().await
}

// Client Metadata Update Prose Test 2: Multiple Successive Metadata Updates
#[tokio::test]
async fn successive_metadata_updates() {
    InitialMetadata::Appended.run_test().await
}
