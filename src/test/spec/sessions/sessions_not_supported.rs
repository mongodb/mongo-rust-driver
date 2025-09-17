use std::time::Duration;

use crate::{
    bson::doc,
    client::options::ClientOptions,
    error::ErrorKind,
    event::command::CommandEvent,
    runtime::process::Process,
    test::{util::Event, EventClient},
    Client,
};

async fn spawn_mongocryptd(name: &str) -> Option<(EventClient, Process)> {
    let pid_file_path = format!("--pidfilepath={name}.pid");
    let args = vec!["--port=47017", &pid_file_path];
    let process = Process::spawn("mongocryptd", args).expect("failed to spawn mongocryptd");

    let options = ClientOptions::parse("mongodb://localhost:47017")
        .await
        .unwrap();
    let client = Client::for_test().options(options).monitor_events().await;
    let hello_response = client.hello().await.unwrap();
    assert!(hello_response.logical_session_timeout_minutes.is_none());

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

    let mut event_stream = client.events.stream();
    let coll = client.database(name).collection(name);

    let _ = coll.find(doc! {}).await;
    let event = event_stream
        .next_map(Duration::from_millis(500), |event| match event {
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
    let event = event_stream
        .next_map(Duration::from_millis(500), |event| match event {
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
