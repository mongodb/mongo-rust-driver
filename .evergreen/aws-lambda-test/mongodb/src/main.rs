use std::sync::Arc;

use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use mongodb::{
    bson::doc,
    event::{cmap::CmapEvent, command::CommandEvent, sdam::SdamEvent, EventHandler},
    options::ClientOptions,
    Client,
};
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

struct State {
    client: Client,
    stats: Arc<std::sync::Mutex<Stats>>,
}

#[derive(Clone, Serialize)]
struct Stats {
    heartbeats_started: u32,
    failed_heartbeat_durations_millis: Vec<u128>,
    command_succeeded_durations_millis: Vec<u128>,
    command_failed_durations_millis: Vec<u128>,
    connections_open: u32,
    max_connections_open: u32,
}

impl Stats {
    fn new() -> Self {
        Self {
            heartbeats_started: 0,
            failed_heartbeat_durations_millis: vec![],
            command_succeeded_durations_millis: vec![],
            command_failed_durations_millis: vec![],
            connections_open: 0,
            max_connections_open: 0,
        }
    }

    fn handle_sdam(&mut self, event: &SdamEvent) {
        match event {
            SdamEvent::ServerHeartbeatStarted(ev) => {
                assert!(!ev.awaited);
                self.heartbeats_started += 1;
            }
            SdamEvent::ServerHeartbeatFailed(ev) => {
                assert!(!ev.awaited);
                self.failed_heartbeat_durations_millis
                    .push(ev.duration.as_millis());
            }
            _ => (),
        }
    }

    fn handle_command(&mut self, event: &CommandEvent) {
        match event {
            CommandEvent::Succeeded(ev) => {
                self.command_succeeded_durations_millis
                    .push(ev.duration.as_millis());
            }
            CommandEvent::Failed(ev) => {
                self.command_failed_durations_millis
                    .push(ev.duration.as_millis());
            }
            _ => (),
        }
    }

    fn handle_cmap(&mut self, event: &CmapEvent) {
        match event {
            CmapEvent::ConnectionCreated(_) => {
                self.connections_open += 1;
                self.max_connections_open =
                    std::cmp::max(self.connections_open, self.max_connections_open);
            }
            CmapEvent::ConnectionClosed(_) => {
                self.connections_open -= 1;
            }
            _ => (),
        }
    }
}

impl State {
    async fn new() -> Self {
        let uri = std::env::var("MONGODB_URI")
            .expect("MONGODB_URI must be set to the URI of the MongoDB deployment");
        let mut options = ClientOptions::parse(uri)
            .await
            .expect("Failed to parse URI");
        let stats = Arc::new(std::sync::Mutex::new(Stats::new()));
        {
            let stats = Arc::clone(&stats);
            options.sdam_event_handler = Some(EventHandler::callback(move |ev| {
                stats.lock().unwrap().handle_sdam(&ev)
            }));
        }
        {
            let stats = Arc::clone(&stats);
            options.command_event_handler = Some(EventHandler::callback(move |ev| {
                stats.lock().unwrap().handle_command(&ev)
            }));
        }
        {
            let stats = Arc::clone(&stats);
            options.cmap_event_handler = Some(EventHandler::callback(move |ev| {
                stats.lock().unwrap().handle_cmap(&ev)
            }));
        }

        let client = Client::with_options(options).expect("Failed to create MongoDB Client");

        Self { client, stats }
    }
}

static STATE: OnceCell<State> = OnceCell::const_new();

async fn get_state() -> &'static State {
    STATE.get_or_init(State::new).await
}

#[derive(Deserialize)]
struct Request {}

async fn function_handler(_event: LambdaEvent<Request>) -> Result<Stats, Error> {
    let state = get_state().await;
    let coll = state.client.database("faas_test").collection("faas_test");
    let id = coll.insert_one(doc! {}).await?.inserted_id;
    coll.delete_one(doc! { "id": id }).await?;

    let stats = {
        let mut guard = state.stats.lock().unwrap();
        let value = guard.clone();
        *guard = Stats::new();
        value
    };
    Ok(stats)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    run(service_fn(function_handler)).await
}
