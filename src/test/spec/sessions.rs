use std::{
    future::IntoFuture,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::TryStreamExt;
use futures_util::{future::try_join_all, FutureExt};

use crate::{
    bson::{doc, Document},
    client::options::ClientOptions,
    error::{ErrorKind, Result},
    event::command::{CommandEvent, CommandStartedEvent},
    runtime::process::Process,
    test::{
        get_client_options,
        log_uncaptured,
        spec::unified_runner::run_unified_tests,
        util::Event,
        EventClient,
        TestClient,
    },
    Client,
};

#[tokio::test(flavor = "multi_thread")]
async fn run_unified() {
    let mut skipped_files = vec![];
    let client = TestClient::new().await;
    if client.is_sharded() && client.server_version_gte(7, 0) {
        // TODO RUST-1666: unskip this file
        skipped_files.push("snapshot-sessions.json");
    }

    run_unified_tests(&["sessions"])
        .skip_files(&skipped_files)
        .await;
}

// Sessions prose test 1
#[tokio::test]
async fn snapshot_and_causal_consistency_are_mutually_exclusive() {
    let client = TestClient::new().await;
    assert!(client
        .start_session()
        .snapshot(true)
        .causal_consistency(true)
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread")]
#[function_name::named]
async fn explicit_session_created_on_same_client() {
    let client0 = TestClient::new().await;
    let client1 = TestClient::new().await;

    let mut session0 = client0.start_session().await.unwrap();
    let mut session1 = client1.start_session().await.unwrap();

    let db = client0.database(function_name!());
    let err = db
        .list_collections()
        .session(&mut session1)
        .await
        .unwrap_err();
    match *err.kind {
        ErrorKind::InvalidArgument { message } => assert!(message.contains("session provided")),
        other => panic!("expected InvalidArgument error, got {:?}", other),
    }

    let coll = client1
        .database(function_name!())
        .collection(function_name!());
    let err = coll
        .insert_one(doc! {})
        .session(&mut session0)
        .await
        .unwrap_err();
    match *err.kind {
        ErrorKind::InvalidArgument { message } => assert!(message.contains("session provided")),
        other => panic!("expected InvalidArgument error, got {:?}", other),
    }
}

// Sessions prose test 14
#[tokio::test]
async fn implicit_session_after_connection() {
    struct EventHandler {
        lsids: Mutex<Vec<Document>>,
    }

    #[allow(deprecated)]
    impl crate::event::command::CommandEventHandler for EventHandler {
        fn handle_command_started_event(&self, event: CommandStartedEvent) {
            self.lsids
                .lock()
                .unwrap()
                .push(event.command.get_document("lsid").unwrap().clone());
        }
    }

    let event_handler = Arc::new(EventHandler {
        lsids: Mutex::new(vec![]),
    });

    let mut min_lsids = usize::MAX;
    let mut max_lsids = 0usize;
    for _ in 0..5 {
        let client = {
            let mut options = get_client_options().await.clone();
            options.max_pool_size = Some(1);
            options.retry_writes = Some(true);
            options.hosts.drain(1..);
            options.command_event_handler = Some(event_handler.clone().into());
            Client::with_options(options).unwrap()
        };

        let coll = client
            .database("test_lazy_implicit")
            .collection::<Document>("test");

        let mut ops = vec![];
        fn ignore_val<T>(r: Result<T>) -> Result<()> {
            r.map(|_| ())
        }
        ops.push(
            coll.insert_one(doc! {})
                .into_future()
                .map(ignore_val)
                .boxed(),
        );
        ops.push(
            coll.delete_one(doc! {})
                .into_future()
                .map(ignore_val)
                .boxed(),
        );
        ops.push(
            coll.update_one(doc! {}, doc! { "$set": { "a": 1 } })
                .into_future()
                .map(ignore_val)
                .boxed(),
        );
        ops.push(
            coll.find_one_and_delete(doc! {})
                .into_future()
                .map(ignore_val)
                .boxed(),
        );
        ops.push(
            coll.find_one_and_update(doc! {}, doc! { "$set": { "a": 1 } })
                .into_future()
                .map(ignore_val)
                .boxed(),
        );
        ops.push(
            coll.find_one_and_replace(doc! {}, doc! { "a": 1 })
                .into_future()
                .map(ignore_val)
                .boxed(),
        );
        ops.push(
            async {
                let cursor = coll.find(doc! {}).await.unwrap();
                let r: Result<Vec<_>> = cursor.try_collect().await;
                r.map(|_| ())
            }
            .boxed(),
        );

        let _ = try_join_all(ops).await.unwrap();

        let mut lsids = event_handler.lsids.lock().unwrap();
        let mut unique = vec![];
        'outer: for lsid in lsids.iter() {
            for u in &unique {
                if lsid == *u {
                    continue 'outer;
                }
            }
            unique.push(lsid);
        }

        min_lsids = std::cmp::min(min_lsids, unique.len());
        max_lsids = std::cmp::max(max_lsids, unique.len());
        lsids.clear();
    }
    // The spec says the minimum should be 1; however, the background async nature of the Rust
    // driver's session cleanup means that sometimes a session has not yet been returned to the pool
    // when the next one is checked out.
    assert!(
        min_lsids <= 2,
        "min lsids is {}, expected <= 2 (max is {})",
        min_lsids,
        max_lsids,
    );
    assert!(
        max_lsids < 7,
        "max lsids is {}, expected < 7 (min is {})",
        max_lsids,
        min_lsids,
    );
}

async fn spawn_mongocryptd(name: &str) -> Option<(EventClient, Process)> {
    let util_client = TestClient::new().await;
    if util_client.server_version_lt(4, 2) {
        log_uncaptured(format!(
            "Skipping {name}: cannot spawn mongocryptd due to server version < 4.2"
        ));
        return None;
    }

    let pid_file_path = format!("--pidfilepath={name}.pid");
    let args = vec!["--port=47017", &pid_file_path];
    let Ok(process) = Process::spawn("mongocryptd", args) else {
        if std::env::var("SESSION_TEST_REQUIRE_MONGOCRYPTD").is_ok() {
            panic!("Failed to spawn mongocryptd");
        }
        log_uncaptured(format!("Skipping {name}: failed to spawn mongocryptd"));
        return None;
    };

    let options = ClientOptions::parse("mongodb://localhost:47017")
        .await
        .unwrap();
    let client = Client::test_builder()
        .options(options)
        .monitor_events()
        .build()
        .await;
    assert!(client.server_info.logical_session_timeout_minutes.is_none());

    Some((client, process))
}

async fn clean_up_mongocryptd(mut process: Process, name: &str) {
    let _ = std::fs::remove_file(format!("{name}.pid"));
    let _ = process.kill();
    let _ = process.wait().await;
}

// Sessions prose test 18
#[tokio::test]
async fn sessions_not_supported_implicit_session_ignored() {
    let name = "sessions_not_supported_implicit_session_ignored";

    let Some((client, process)) = spawn_mongocryptd(name).await else {
        return;
    };

    #[allow(deprecated)]
    let mut subscriber = client.events.subscribe();
    let coll = client.database(name).collection(name);

    let _ = coll.find(doc! {}).await;
    let event = subscriber
        .filter_map_event(Duration::from_millis(500), |event| match event {
            Event::Command(CommandEvent::Started(command_started_event))
                if command_started_event.command_name == "find" =>
            {
                Some(command_started_event)
            }
            _ => None,
        })
        .await
        .expect("Did not observe a command started event for find operation");
    assert!(!event.command.contains_key("lsid"));

    let _ = coll.insert_one(doc! { "x": 1 }).await;
    let event = subscriber
        .filter_map_event(Duration::from_millis(500), |event| match event {
            Event::Command(CommandEvent::Started(command_started_event))
                if command_started_event.command_name == "insert" =>
            {
                Some(command_started_event)
            }
            _ => None,
        })
        .await
        .expect("Did not observe a command started event for insert operation");
    assert!(!event.command.contains_key("lsid"));

    clean_up_mongocryptd(process, name).await;
}

// Sessions prose test 19
#[tokio::test]
async fn sessions_not_supported_explicit_session_error() {
    let name = "sessions_not_supported_explicit_session_error";

    let Some((client, process)) = spawn_mongocryptd(name).await else {
        return;
    };

    let mut session = client.start_session().await.unwrap();
    let coll = client.database(name).collection(name);

    let error = coll
        .find_one(doc! {})
        .session(&mut session)
        .await
        .unwrap_err();
    assert!(matches!(*error.kind, ErrorKind::SessionsNotSupported));

    let error = coll
        .insert_one(doc! { "x": 1 })
        .session(&mut session)
        .await
        .unwrap_err();
    assert!(matches!(*error.kind, ErrorKind::SessionsNotSupported));

    clean_up_mongocryptd(process, name).await;
}
