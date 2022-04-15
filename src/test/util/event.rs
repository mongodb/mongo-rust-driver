use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use tokio::sync::{
    broadcast::error::{RecvError, SendError},
    RwLockReadGuard,
};

use super::TestClient;
use crate::{
    bson::doc,
    event::{
        cmap::{
            CmapEventHandler,
            ConnectionCheckedInEvent,
            ConnectionCheckedOutEvent,
            ConnectionCheckoutFailedEvent,
            ConnectionCheckoutStartedEvent,
            ConnectionClosedEvent,
            ConnectionCreatedEvent,
            ConnectionReadyEvent,
            PoolClearedEvent,
            PoolClosedEvent,
            PoolCreatedEvent,
            PoolReadyEvent,
        },
        command::{
            CommandEventHandler,
            CommandFailedEvent,
            CommandStartedEvent,
            CommandSucceededEvent,
        },
        sdam::{
            SdamEventHandler,
            ServerClosedEvent,
            ServerDescriptionChangedEvent,
            ServerHeartbeatFailedEvent,
            ServerHeartbeatStartedEvent,
            ServerHeartbeatSucceededEvent,
            ServerOpeningEvent,
            TopologyClosedEvent,
            TopologyDescriptionChangedEvent,
            TopologyOpeningEvent,
        },
    },
    options::ClientOptions,
    runtime,
    test::{spec::ExpectedEventType, LOCK},
};

pub type EventQueue<T> = Arc<RwLock<VecDeque<T>>>;
pub type CmapEvent = crate::cmap::test::event::Event;

#[derive(Clone, Debug, From)]
#[allow(clippy::large_enum_variant)]
pub enum Event {
    Cmap(CmapEvent),
    Command(CommandEvent),
    Sdam(SdamEvent),
}

impl Event {
    pub fn unwrap_sdam_event(self) -> SdamEvent {
        if let Event::Sdam(e) = self {
            e
        } else {
            panic!("expected SDAM event, instead got {:#?}", self)
        }
    }
}

#[derive(Clone, Debug)]
pub enum SdamEvent {
    ServerDescriptionChanged(Box<ServerDescriptionChangedEvent>),
    ServerOpening(ServerOpeningEvent),
    ServerClosed(ServerClosedEvent),
    TopologyDescriptionChanged(Box<TopologyDescriptionChangedEvent>),
    TopologyOpening(TopologyOpeningEvent),
    TopologyClosed(TopologyClosedEvent),
    ServerHeartbeatStarted(ServerHeartbeatStartedEvent),
    ServerHeartbeatSucceeded(ServerHeartbeatSucceededEvent),
    ServerHeartbeatFailed(ServerHeartbeatFailedEvent),
}

#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum CommandEvent {
    Started(CommandStartedEvent),
    Succeeded(CommandSucceededEvent),
    Failed(CommandFailedEvent),
}

impl CommandEvent {
    pub fn command_name(&self) -> &str {
        match self {
            CommandEvent::Started(event) => event.command_name.as_str(),
            CommandEvent::Failed(event) => event.command_name.as_str(),
            CommandEvent::Succeeded(event) => event.command_name.as_str(),
        }
    }

    fn request_id(&self) -> i32 {
        match self {
            CommandEvent::Started(event) => event.request_id,
            CommandEvent::Failed(event) => event.request_id,
            CommandEvent::Succeeded(event) => event.request_id,
        }
    }

    pub fn as_command_started(&self) -> Option<&CommandStartedEvent> {
        match self {
            CommandEvent::Started(e) => Some(e),
            _ => None,
        }
    }

    pub fn as_command_succeeded(&self) -> Option<&CommandSucceededEvent> {
        match self {
            CommandEvent::Succeeded(e) => Some(e),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct EventHandler {
    command_events: EventQueue<CommandEvent>,
    sdam_events: EventQueue<SdamEvent>,
    cmap_events: EventQueue<CmapEvent>,
    event_broadcaster: tokio::sync::broadcast::Sender<Event>,
    connections_checked_out: Arc<Mutex<u32>>,
}

impl EventHandler {
    pub fn new() -> Self {
        let (event_broadcaster, _) = tokio::sync::broadcast::channel(500);
        Self {
            command_events: Default::default(),
            sdam_events: Default::default(),
            cmap_events: Default::default(),
            event_broadcaster,
            connections_checked_out: Arc::new(Mutex::new(0)),
        }
    }

    fn handle(&self, event: impl Into<Event>) {
        // this only errors if no receivers are listening which isn't a concern here.
        let _: std::result::Result<usize, SendError<Event>> =
            self.event_broadcaster.send(event.into());
    }

    pub fn subscribe(&self) -> EventSubscriber {
        EventSubscriber {
            _handler: self,
            receiver: self.event_broadcaster.subscribe(),
        }
    }

    /// Gets all of the command started events for the specified command names.
    pub fn get_command_started_events(&self, command_names: &[&str]) -> Vec<CommandStartedEvent> {
        let events = self.command_events.read().unwrap();
        events
            .iter()
            .filter_map(|event| match event {
                CommandEvent::Started(event) => {
                    if command_names.contains(&event.command_name.as_str()) {
                        Some(event.clone())
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect()
    }

    /// Gets all of the command started events, excluding configureFailPoint events.
    pub fn get_all_command_started_events(&self) -> Vec<CommandStartedEvent> {
        let events = self.command_events.read().unwrap();
        events
            .iter()
            .filter_map(|event| match event {
                CommandEvent::Started(event) if event.command_name != "configureFailPoint" => {
                    Some(event.clone())
                }
                _ => None,
            })
            .collect()
    }

    pub fn get_filtered_events<F>(&self, event_type: ExpectedEventType, filter: F) -> Vec<Event>
    where
        F: Fn(&Event) -> bool,
    {
        match event_type {
            ExpectedEventType::Command => {
                let events = self.command_events.read().unwrap();
                events
                    .iter()
                    .cloned()
                    .map(Event::Command)
                    .filter(|e| filter(e))
                    .collect()
            }
            ExpectedEventType::Cmap => {
                let events = self.cmap_events.read().unwrap();
                events
                    .iter()
                    .cloned()
                    .map(Event::Cmap)
                    .filter(|e| filter(e))
                    .collect()
            }
            ExpectedEventType::CmapWithoutConnectionReady => {
                let mut events = self.get_filtered_events(ExpectedEventType::Cmap, filter);
                events.retain(|ev| !matches!(ev, Event::Cmap(CmapEvent::ConnectionReady(_))));
                events
            }
        }
    }

    pub fn connections_checked_out(&self) -> u32 {
        *self.connections_checked_out.lock().unwrap()
    }

    pub fn clear_cached_events(&self) {
        self.command_events.write().unwrap().clear();
        self.cmap_events.write().unwrap().clear();
        self.sdam_events.write().unwrap().clear();
    }
}

impl CmapEventHandler for EventHandler {
    fn handle_connection_checked_out_event(&self, event: ConnectionCheckedOutEvent) {
        *self.connections_checked_out.lock().unwrap() += 1;
        let event = CmapEvent::ConnectionCheckedOut(event);
        self.handle(event.clone());
        self.cmap_events.write().unwrap().push_back(event);
    }

    fn handle_connection_checkout_failed_event(&self, event: ConnectionCheckoutFailedEvent) {
        let event = CmapEvent::ConnectionCheckOutFailed(event);
        self.handle(event.clone());
        self.cmap_events.write().unwrap().push_back(event);
    }

    fn handle_pool_cleared_event(&self, pool_cleared_event: PoolClearedEvent) {
        let event = CmapEvent::PoolCleared(pool_cleared_event);
        self.handle(event.clone());
        self.cmap_events.write().unwrap().push_back(event);
    }

    fn handle_pool_ready_event(&self, event: PoolReadyEvent) {
        let event = CmapEvent::PoolReady(event);
        self.handle(event.clone());
        self.cmap_events.write().unwrap().push_back(event);
    }

    fn handle_pool_created_event(&self, event: PoolCreatedEvent) {
        let event = CmapEvent::PoolCreated(event);
        self.handle(event.clone());
        self.cmap_events.write().unwrap().push_back(event);
    }

    fn handle_pool_closed_event(&self, event: PoolClosedEvent) {
        let event = CmapEvent::PoolClosed(event);
        self.handle(event.clone());
        self.cmap_events.write().unwrap().push_back(event);
    }

    fn handle_connection_created_event(&self, event: ConnectionCreatedEvent) {
        let event = CmapEvent::ConnectionCreated(event);
        self.handle(event.clone());
        self.cmap_events.write().unwrap().push_back(event);
    }

    fn handle_connection_ready_event(&self, event: ConnectionReadyEvent) {
        let event = CmapEvent::ConnectionReady(event);
        self.handle(event.clone());
        self.cmap_events.write().unwrap().push_back(event);
    }

    fn handle_connection_closed_event(&self, event: ConnectionClosedEvent) {
        let event = CmapEvent::ConnectionClosed(event);
        self.handle(event.clone());
        self.cmap_events.write().unwrap().push_back(event);
    }

    fn handle_connection_checkout_started_event(&self, event: ConnectionCheckoutStartedEvent) {
        let event = CmapEvent::ConnectionCheckOutStarted(event);
        self.handle(event.clone());
        self.cmap_events.write().unwrap().push_back(event);
    }

    fn handle_connection_checked_in_event(&self, event: ConnectionCheckedInEvent) {
        *self.connections_checked_out.lock().unwrap() -= 1;
        let event = CmapEvent::ConnectionCheckedIn(event);
        self.handle(event.clone());
        self.cmap_events.write().unwrap().push_back(event);
    }
}

impl SdamEventHandler for EventHandler {
    fn handle_server_description_changed_event(&self, event: ServerDescriptionChangedEvent) {
        let event = SdamEvent::ServerDescriptionChanged(Box::new(event));
        self.handle(event.clone());
        self.sdam_events.write().unwrap().push_back(event);
    }

    fn handle_server_opening_event(&self, event: ServerOpeningEvent) {
        let event = SdamEvent::ServerOpening(event);
        self.handle(event.clone());
        self.sdam_events.write().unwrap().push_back(event);
    }

    fn handle_server_closed_event(&self, event: ServerClosedEvent) {
        let event = SdamEvent::ServerClosed(event);
        self.handle(event.clone());
        self.sdam_events.write().unwrap().push_back(event);
    }

    fn handle_topology_description_changed_event(&self, event: TopologyDescriptionChangedEvent) {
        let event = SdamEvent::TopologyDescriptionChanged(Box::new(event));
        self.handle(event.clone());
        self.sdam_events.write().unwrap().push_back(event);
    }

    fn handle_topology_opening_event(&self, event: TopologyOpeningEvent) {
        let event = SdamEvent::TopologyOpening(event);
        self.handle(event.clone());
        self.sdam_events.write().unwrap().push_back(event);
    }

    fn handle_topology_closed_event(&self, event: TopologyClosedEvent) {
        let event = SdamEvent::TopologyClosed(event);
        self.handle(event.clone());
        self.sdam_events.write().unwrap().push_back(event);
    }

    fn handle_server_heartbeat_started_event(&self, event: ServerHeartbeatStartedEvent) {
        let event = SdamEvent::ServerHeartbeatStarted(event);
        self.handle(event.clone());
        self.sdam_events.write().unwrap().push_back(event);
    }

    fn handle_server_heartbeat_succeeded_event(&self, event: ServerHeartbeatSucceededEvent) {
        let event = SdamEvent::ServerHeartbeatSucceeded(event);
        self.handle(event.clone());
        self.sdam_events.write().unwrap().push_back(event);
    }

    fn handle_server_heartbeat_failed_event(&self, event: ServerHeartbeatFailedEvent) {
        let event = SdamEvent::ServerHeartbeatFailed(event);
        self.handle(event.clone());
        self.sdam_events.write().unwrap().push_back(event);
    }
}

impl CommandEventHandler for EventHandler {
    fn handle_command_started_event(&self, event: CommandStartedEvent) {
        self.handle(CommandEvent::Started(event.clone()));
        self.command_events
            .write()
            .unwrap()
            .push_back(CommandEvent::Started(event))
    }

    fn handle_command_failed_event(&self, event: CommandFailedEvent) {
        self.handle(CommandEvent::Failed(event.clone()));
        self.command_events
            .write()
            .unwrap()
            .push_back(CommandEvent::Failed(event))
    }

    fn handle_command_succeeded_event(&self, event: CommandSucceededEvent) {
        self.handle(CommandEvent::Succeeded(event.clone()));
        self.command_events
            .write()
            .unwrap()
            .push_back(CommandEvent::Succeeded(event))
    }
}

#[derive(Debug)]
pub struct EventSubscriber<'a> {
    /// A reference to the handler this subscriber is receiving events from.
    /// Stored here to ensure this subscriber cannot outlive the handler that is generating its
    /// events.
    _handler: &'a EventHandler,
    receiver: tokio::sync::broadcast::Receiver<Event>,
}

impl<'a> EventSubscriber<'a> {
    pub async fn wait_for_event<F>(&mut self, timeout: Duration, filter: F) -> Option<Event>
    where
        F: Fn(&Event) -> bool,
    {
        runtime::timeout(timeout, async {
            loop {
                match self.receiver.recv().await {
                    Ok(event) if filter(&event) => return event.into(),
                    // the channel hit capacity and the channel will skip a few to catch up.
                    Err(RecvError::Lagged(_)) => continue,
                    Err(_) => return None,
                    _ => continue,
                }
            }
        })
        .await
        .ok()
        .flatten()
    }

    pub async fn collect_events<F>(&mut self, timeout: Duration, filter: F) -> Vec<Event>
    where
        F: Fn(&Event) -> bool,
    {
        let mut events = Vec::new();
        while let Some(event) = self.wait_for_event(timeout, &filter).await {
            events.push(event);
        }
        events
    }
}

#[derive(Clone, Debug)]
pub struct EventClient {
    client: TestClient,
    pub(crate) handler: Arc<EventHandler>,
}

impl std::ops::Deref for EventClient {
    type Target = TestClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl std::ops::DerefMut for EventClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

impl EventClient {
    pub async fn new() -> Self {
        EventClient::with_options(None).await
    }

    async fn with_options_and_handler(
        options: impl Into<Option<ClientOptions>>,
        handler: impl Into<Option<EventHandler>>,
    ) -> Self {
        let handler = Arc::new(handler.into().unwrap_or_else(EventHandler::new));
        let client = TestClient::with_handler(Some(handler.clone()), options).await;

        // clear events from commands used to set up client.
        handler.command_events.write().unwrap().clear();

        Self { client, handler }
    }

    pub async fn with_options(options: impl Into<Option<ClientOptions>>) -> Self {
        Self::with_options_and_handler(options, None).await
    }

    pub async fn with_additional_options(
        options: impl Into<Option<ClientOptions>>,
        heartbeat_freq: Option<Duration>,
        use_multiple_mongoses: Option<bool>,
        event_handler: impl Into<Option<EventHandler>>,
    ) -> Self {
        let mut options = TestClient::options_for_multiple_mongoses(
            options.into(),
            use_multiple_mongoses.unwrap_or(false),
        )
        .await;
        options.test_options_mut().heartbeat_freq = heartbeat_freq;
        EventClient::with_options_and_handler(options, event_handler).await
    }

    /// Gets the first started/succeeded pair of events for the given command name, popping off all
    /// events before and between them.
    ///
    /// Panics if the command failed or could not be found in the events.
    pub fn get_successful_command_execution(
        &self,
        command_name: &str,
    ) -> (CommandStartedEvent, CommandSucceededEvent) {
        let mut command_events = self.handler.command_events.write().unwrap();

        let mut started: Option<CommandStartedEvent> = None;

        while let Some(event) = command_events.pop_front() {
            if event.command_name() == command_name {
                match started {
                    None => {
                        let event = event
                            .as_command_started()
                            .unwrap_or_else(|| {
                                panic!("first event not a command started event {:?}", event)
                            })
                            .clone();
                        started = Some(event);
                        continue;
                    }
                    Some(started) if event.request_id() == started.request_id => {
                        let succeeded = event
                            .as_command_succeeded()
                            .expect("second event not a command succeeded event")
                            .clone();

                        return (started, succeeded);
                    }
                    _ => continue,
                }
            }
        }
        panic!("could not find event for {} command", command_name);
    }

    /// Gets all of the command started events for the specified command names.
    pub fn get_command_started_events(&self, command_names: &[&str]) -> Vec<CommandStartedEvent> {
        self.handler.get_command_started_events(command_names)
    }

    /// Gets all command started events, excluding configureFailPoint events.
    pub fn get_all_command_started_events(&self) -> Vec<CommandStartedEvent> {
        self.handler.get_all_command_started_events()
    }

    pub fn get_command_events(&self, command_names: &[&str]) -> Vec<CommandEvent> {
        self.handler
            .command_events
            .write()
            .unwrap()
            .drain(..)
            .filter(|event| command_names.contains(&event.command_name()))
            .collect()
    }

    pub fn count_pool_cleared_events(&self) -> usize {
        let mut out = 0;
        for event in self.handler.cmap_events.read().unwrap().iter() {
            if matches!(event, CmapEvent::PoolCleared(_)) {
                out += 1;
            }
        }
        out
    }

    #[allow(dead_code)]
    pub fn subscribe_to_events(&self) -> EventSubscriber<'_> {
        self.handler.subscribe()
    }

    pub fn clear_cached_events(&self) {
        self.handler.clear_cached_events()
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn command_started_event_count() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = EventClient::new().await;
    let coll = client.database("foo").collection("bar");

    for i in 0..10 {
        coll.insert_one(doc! { "x": i }, None).await.unwrap();
    }

    assert_eq!(client.get_command_started_events(&["insert"]).len(), 10);
}
