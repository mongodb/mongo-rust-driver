use std::{
    collections::VecDeque,
    fs::File,
    io::{BufWriter, Write},
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use derive_more::From;
use time::OffsetDateTime;
use tokio::sync::broadcast::error::SendError;

use super::{subscriber::EventSubscriber, TestClient, TestClientBuilder};
use crate::{
    bson::{doc, to_document, Document},
    event::{
        cmap::{
            CmapEvent,
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
        command::{CommandEvent, CommandFailedEvent, CommandStartedEvent, CommandSucceededEvent},
        sdam::{
            SdamEvent,
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
    test::spec::ExpectedEventType,
    Client,
};

pub(crate) type EventQueue<T> = Arc<RwLock<VecDeque<(T, OffsetDateTime)>>>;

fn add_event_to_queue<T>(event_queue: &EventQueue<T>, event: T) {
    event_queue
        .write()
        .unwrap()
        .push_back((event, OffsetDateTime::now_utc()))
}

#[derive(Clone, Debug, From)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum Event {
    Cmap(CmapEvent),
    Command(CommandEvent),
    Sdam(SdamEvent),
}

impl Event {
    pub(crate) fn unwrap_sdam_event(self) -> SdamEvent {
        if let Event::Sdam(e) = self {
            e
        } else {
            panic!("expected SDAM event, instead got {:#?}", self)
        }
    }

    #[cfg(feature = "in-use-encryption-unstable")]
    pub(crate) fn as_command_started_event(&self) -> Option<&CommandStartedEvent> {
        match self {
            Event::Command(CommandEvent::Started(e)) => Some(e),
            _ => None,
        }
    }

    #[cfg(feature = "in-use-encryption-unstable")]
    pub(crate) fn into_command_started_event(self) -> Option<CommandStartedEvent> {
        match self {
            Self::Command(CommandEvent::Started(ev)) => Some(ev),
            _ => None,
        }
    }
}

impl SdamEvent {
    pub fn name(&self) -> &str {
        match self {
            Self::ServerDescriptionChanged(_) => "ServerDescriptionChangedEvent",
            Self::ServerOpening(_) => "ServerOpeningEvent",
            Self::ServerClosed(_) => "ServerClosedEvent",
            Self::TopologyDescriptionChanged(_) => "TopologyDescriptionChanged",
            Self::TopologyOpening(_) => "TopologyOpeningEvent",
            Self::TopologyClosed(_) => "TopologyClosedEvent",
            Self::ServerHeartbeatStarted(_) => "ServerHeartbeatStartedEvent",
            Self::ServerHeartbeatSucceeded(_) => "ServerHeartbeatSucceededEvent",
            Self::ServerHeartbeatFailed(_) => "ServerHeartbeatFailedEvent",
        }
    }
}

impl CommandEvent {
    pub fn name(&self) -> &str {
        match self {
            Self::Started(_) => "CommandStartedEvent",
            Self::Succeeded(_) => "CommandSucceededEvent",
            Self::Failed(_) => "CommandFailedEvent",
        }
    }

    pub(crate) fn command_name(&self) -> &str {
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

    pub(crate) fn as_command_started(&self) -> Option<&CommandStartedEvent> {
        match self {
            CommandEvent::Started(e) => Some(e),
            _ => None,
        }
    }

    pub(crate) fn as_command_succeeded(&self) -> Option<&CommandSucceededEvent> {
        match self {
            CommandEvent::Succeeded(e) => Some(e),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct EventHandler {
    command_events: EventQueue<CommandEvent>,
    sdam_events: EventQueue<SdamEvent>,
    cmap_events: EventQueue<CmapEvent>,
    event_broadcaster: tokio::sync::broadcast::Sender<Event>,
    connections_checked_out: Arc<Mutex<u32>>,
}

impl EventHandler {
    pub(crate) fn new() -> Self {
        let (event_broadcaster, _) = tokio::sync::broadcast::channel(10_000);
        Self {
            command_events: Default::default(),
            sdam_events: Default::default(),
            cmap_events: Default::default(),
            event_broadcaster,
            connections_checked_out: Arc::new(Mutex::new(0)),
        }
    }

    pub(crate) fn command_sender(self: Arc<Self>) -> tokio::sync::mpsc::Sender<CommandEvent> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<CommandEvent>(100);
        crate::runtime::spawn(async move {
            while let Some(ev) = rx.recv().await {
                self.handle(ev.clone());
                add_event_to_queue(&self.command_events, ev);
            }
        });
        tx
    }

    pub(crate) fn cmap_sender(self: Arc<Self>) -> tokio::sync::mpsc::Sender<CmapEvent> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<CmapEvent>(100);
        crate::runtime::spawn(async move {
            while let Some(ev) = rx.recv().await {
                match &ev {
                    CmapEvent::ConnectionCheckedOut(_) => {
                        *self.connections_checked_out.lock().unwrap() += 1
                    }
                    CmapEvent::ConnectionCheckedIn(_) => {
                        *self.connections_checked_out.lock().unwrap() -= 1
                    }
                    _ => (),
                }
                self.handle(ev.clone());
                add_event_to_queue(&self.cmap_events, ev);
            }
        });
        tx
    }

    pub(crate) fn sdam_sender(self: Arc<Self>) -> tokio::sync::mpsc::Sender<SdamEvent> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<SdamEvent>(100);
        crate::runtime::spawn(async move {
            while let Some(ev) = rx.recv().await {
                self.handle(ev.clone());
                add_event_to_queue(&self.sdam_events, ev);
            }
        });
        tx
    }

    fn handle(&self, event: impl Into<Event>) {
        // this only errors if no receivers are listening which isn't a concern here.
        let _: std::result::Result<usize, SendError<Event>> =
            self.event_broadcaster.send(event.into());
    }

    pub(crate) fn subscribe(&self) -> EventSubscriber<EventHandler, Event> {
        EventSubscriber::new(self, self.event_broadcaster.subscribe())
    }

    pub(crate) fn broadcaster(&self) -> &tokio::sync::broadcast::Sender<Event> {
        &self.event_broadcaster
    }

    /// Gets all of the command started events for the specified command names.
    pub(crate) fn get_command_started_events(
        &self,
        command_names: &[&str],
    ) -> Vec<CommandStartedEvent> {
        let events = self.command_events.read().unwrap();
        events
            .iter()
            .filter_map(|(event, _)| match event {
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
    pub(crate) fn get_all_command_started_events(&self) -> Vec<CommandStartedEvent> {
        let events = self.command_events.read().unwrap();
        events
            .iter()
            .filter_map(|(event, _)| match event {
                CommandEvent::Started(event) if event.command_name != "configureFailPoint" => {
                    Some(event.clone())
                }
                _ => None,
            })
            .collect()
    }

    pub(crate) fn get_filtered_events<F>(
        &self,
        event_type: ExpectedEventType,
        filter: F,
    ) -> Vec<Event>
    where
        F: Fn(&Event) -> bool,
    {
        match event_type {
            ExpectedEventType::Command => {
                let events = self.command_events.read().unwrap();
                events
                    .iter()
                    .cloned()
                    .map(|(event, _)| Event::Command(event))
                    .filter(|e| filter(e))
                    .collect()
            }
            ExpectedEventType::Cmap => {
                let events = self.cmap_events.read().unwrap();
                events
                    .iter()
                    .cloned()
                    .map(|(event, _)| Event::Cmap(event))
                    .filter(|e| filter(e))
                    .collect()
            }
            ExpectedEventType::CmapWithoutConnectionReady => {
                let mut events = self.get_filtered_events(ExpectedEventType::Cmap, filter);
                events.retain(|ev| !matches!(ev, Event::Cmap(CmapEvent::ConnectionReady(_))));
                events
            }
            ExpectedEventType::Sdam => {
                let events = self.sdam_events.read().unwrap();
                events
                    .iter()
                    .cloned()
                    .map(|(event, _)| Event::Sdam(event))
                    .filter(filter)
                    .collect()
            }
        }
    }

    pub(crate) fn write_events_list_to_file(&self, names: &[&str], writer: &mut BufWriter<File>) {
        let mut add_comma = false;
        let mut write_json = |mut event: Document, name: &str, time: &OffsetDateTime| {
            event.insert("name", name);
            event.insert("observedAt", time.unix_timestamp());
            let mut json_string = serde_json::to_string(&event).unwrap();
            if add_comma {
                json_string.insert(0, ',');
            } else {
                add_comma = true;
            }
            write!(writer, "{}", json_string).unwrap();
        };

        for (command_event, time) in self.command_events.read().unwrap().iter() {
            let name = command_event.name();
            if names.contains(&name) {
                let event = to_document(&command_event).unwrap();
                write_json(event, name, time);
            }
        }
        for (sdam_event, time) in self.sdam_events.read().unwrap().iter() {
            let name = sdam_event.name();
            if names.contains(&name) {
                let event = to_document(&sdam_event).unwrap();
                write_json(event, name, time);
            }
        }
        for (cmap_event, time) in self.cmap_events.read().unwrap().iter() {
            let name = cmap_event.planned_maintenance_testing_name();
            if names.contains(&name) {
                let event = to_document(&cmap_event).unwrap();
                write_json(event, name, time);
            }
        }
    }

    pub(crate) fn connections_checked_out(&self) -> u32 {
        *self.connections_checked_out.lock().unwrap()
    }

    pub(crate) fn clear_cached_events(&self) {
        self.command_events.write().unwrap().clear();
        self.cmap_events.write().unwrap().clear();
        self.sdam_events.write().unwrap().clear();
    }
}

#[allow(deprecated)]
impl crate::event::cmap::CmapEventHandler for EventHandler {
    fn handle_connection_checked_out_event(&self, event: ConnectionCheckedOutEvent) {
        *self.connections_checked_out.lock().unwrap() += 1;
        let event = CmapEvent::ConnectionCheckedOut(event);
        self.handle(event.clone());
        add_event_to_queue(&self.cmap_events, event);
    }

    fn handle_connection_checkout_failed_event(&self, event: ConnectionCheckoutFailedEvent) {
        let event = CmapEvent::ConnectionCheckoutFailed(event);
        self.handle(event.clone());
        add_event_to_queue(&self.cmap_events, event);
    }

    fn handle_pool_cleared_event(&self, pool_cleared_event: PoolClearedEvent) {
        let event = CmapEvent::PoolCleared(pool_cleared_event);
        self.handle(event.clone());
        add_event_to_queue(&self.cmap_events, event);
    }

    fn handle_pool_ready_event(&self, event: PoolReadyEvent) {
        let event = CmapEvent::PoolReady(event);
        self.handle(event.clone());
        add_event_to_queue(&self.cmap_events, event);
    }

    fn handle_pool_created_event(&self, event: PoolCreatedEvent) {
        let event = CmapEvent::PoolCreated(event);
        self.handle(event.clone());
        add_event_to_queue(&self.cmap_events, event);
    }

    fn handle_pool_closed_event(&self, event: PoolClosedEvent) {
        let event = CmapEvent::PoolClosed(event);
        self.handle(event.clone());
        add_event_to_queue(&self.cmap_events, event);
    }

    fn handle_connection_created_event(&self, event: ConnectionCreatedEvent) {
        let event = CmapEvent::ConnectionCreated(event);
        self.handle(event.clone());
        add_event_to_queue(&self.cmap_events, event);
    }

    fn handle_connection_ready_event(&self, event: ConnectionReadyEvent) {
        let event = CmapEvent::ConnectionReady(event);
        self.handle(event.clone());
        add_event_to_queue(&self.cmap_events, event);
    }

    fn handle_connection_closed_event(&self, event: ConnectionClosedEvent) {
        let event = CmapEvent::ConnectionClosed(event);
        self.handle(event.clone());
        add_event_to_queue(&self.cmap_events, event);
    }

    fn handle_connection_checkout_started_event(&self, event: ConnectionCheckoutStartedEvent) {
        let event = CmapEvent::ConnectionCheckoutStarted(event);
        self.handle(event.clone());
        add_event_to_queue(&self.cmap_events, event);
    }

    fn handle_connection_checked_in_event(&self, event: ConnectionCheckedInEvent) {
        *self.connections_checked_out.lock().unwrap() -= 1;
        let event = CmapEvent::ConnectionCheckedIn(event);
        self.handle(event.clone());
        add_event_to_queue(&self.cmap_events, event);
    }
}

#[allow(deprecated)]
impl crate::event::sdam::SdamEventHandler for EventHandler {
    fn handle_server_description_changed_event(&self, event: ServerDescriptionChangedEvent) {
        let event = SdamEvent::ServerDescriptionChanged(Box::new(event));
        self.handle(event.clone());
        add_event_to_queue(&self.sdam_events, event);
    }

    fn handle_server_opening_event(&self, event: ServerOpeningEvent) {
        let event = SdamEvent::ServerOpening(event);
        self.handle(event.clone());
        add_event_to_queue(&self.sdam_events, event);
    }

    fn handle_server_closed_event(&self, event: ServerClosedEvent) {
        let event = SdamEvent::ServerClosed(event);
        self.handle(event.clone());
        add_event_to_queue(&self.sdam_events, event);
    }

    fn handle_topology_description_changed_event(&self, event: TopologyDescriptionChangedEvent) {
        let event = SdamEvent::TopologyDescriptionChanged(Box::new(event));
        self.handle(event.clone());
        add_event_to_queue(&self.sdam_events, event);
    }

    fn handle_topology_opening_event(&self, event: TopologyOpeningEvent) {
        let event = SdamEvent::TopologyOpening(event);
        self.handle(event.clone());
        add_event_to_queue(&self.sdam_events, event);
    }

    fn handle_topology_closed_event(&self, event: TopologyClosedEvent) {
        let event = SdamEvent::TopologyClosed(event);
        self.handle(event.clone());
        add_event_to_queue(&self.sdam_events, event);
    }

    fn handle_server_heartbeat_started_event(&self, event: ServerHeartbeatStartedEvent) {
        let event = SdamEvent::ServerHeartbeatStarted(event);
        self.handle(event.clone());
        add_event_to_queue(&self.sdam_events, event);
    }

    fn handle_server_heartbeat_succeeded_event(&self, event: ServerHeartbeatSucceededEvent) {
        let event = SdamEvent::ServerHeartbeatSucceeded(event);
        self.handle(event.clone());
        add_event_to_queue(&self.sdam_events, event);
    }

    fn handle_server_heartbeat_failed_event(&self, event: ServerHeartbeatFailedEvent) {
        let event = SdamEvent::ServerHeartbeatFailed(event);
        self.handle(event.clone());
        add_event_to_queue(&self.sdam_events, event);
    }
}

#[allow(deprecated)]
impl crate::event::command::CommandEventHandler for EventHandler {
    fn handle_command_started_event(&self, event: CommandStartedEvent) {
        let event = CommandEvent::Started(event);
        self.handle(event.clone());
        add_event_to_queue(&self.command_events, event);
    }

    fn handle_command_failed_event(&self, event: CommandFailedEvent) {
        let event = CommandEvent::Failed(event);
        self.handle(event.clone());
        add_event_to_queue(&self.command_events, event);
    }

    fn handle_command_succeeded_event(&self, event: CommandSucceededEvent) {
        let event = CommandEvent::Succeeded(event);
        self.handle(event.clone());
        add_event_to_queue(&self.command_events, event);
    }
}

impl EventSubscriber<'_, EventHandler, Event> {
    /// Waits for the next CommandStartedEvent/CommandFailedEvent pair.
    /// If the next CommandStartedEvent is associated with a CommandFailedEvent, this method will
    /// panic.
    pub(crate) async fn wait_for_successful_command_execution(
        &mut self,
        timeout: Duration,
        command_name: impl AsRef<str>,
    ) -> Option<(CommandStartedEvent, CommandSucceededEvent)> {
        runtime::timeout(timeout, async {
            let started = self
                .filter_map_event(Duration::MAX, |event| match event {
                    Event::Command(CommandEvent::Started(s))
                        if s.command_name == command_name.as_ref() =>
                    {
                        Some(s)
                    }
                    _ => None,
                })
                .await
                .unwrap();

            let succeeded = self
                .filter_map_event(Duration::MAX, |event| match event {
                    Event::Command(CommandEvent::Succeeded(s))
                        if s.request_id == started.request_id =>
                    {
                        Some(s)
                    }
                    Event::Command(CommandEvent::Failed(f))
                        if f.request_id == started.request_id =>
                    {
                        panic!(
                            "expected {} to succeed but it failed: {:#?}",
                            command_name.as_ref(),
                            f
                        )
                    }
                    _ => None,
                })
                .await
                .unwrap();

            (started, succeeded)
        })
        .await
        .ok()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct EventClient {
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

impl TestClientBuilder {
    pub(crate) fn event_client(self) -> EventClientBuilder {
        EventClientBuilder { inner: self }
    }
}

pub(crate) struct EventClientBuilder {
    inner: TestClientBuilder,
}

impl EventClientBuilder {
    pub(crate) async fn build(self) -> EventClient {
        let mut inner = self.inner;
        if inner.handler.is_none() {
            inner = inner.event_handler(Arc::new(EventHandler::new()));
        }
        let handler = inner.handler().unwrap().clone();
        let client = inner.build().await;

        // clear events from commands used to set up client.
        handler.command_events.write().unwrap().clear();

        EventClient { client, handler }
    }
}

impl EventClient {
    pub(crate) async fn new() -> Self {
        EventClient::with_options(None).await
    }

    async fn with_options_and_handler(
        options: impl Into<Option<ClientOptions>>,
        handler: impl Into<Option<EventHandler>>,
    ) -> Self {
        Client::test_builder()
            .options(options)
            .event_handler(handler.into().map(Arc::new))
            .event_client()
            .build()
            .await
    }

    pub(crate) async fn with_options(options: impl Into<Option<ClientOptions>>) -> Self {
        Self::with_options_and_handler(options, None).await
    }

    pub(crate) async fn with_additional_options(
        options: impl Into<Option<ClientOptions>>,
        min_heartbeat_freq: Option<Duration>,
        use_multiple_mongoses: Option<bool>,
        event_handler: impl Into<Option<EventHandler>>,
    ) -> Self {
        Client::test_builder()
            .additional_options(options, use_multiple_mongoses.unwrap_or(false))
            .await
            .min_heartbeat_freq(min_heartbeat_freq)
            .event_handler(event_handler.into().map(Arc::new))
            .event_client()
            .build()
            .await
    }

    /// Gets the first started/succeeded pair of events for the given command name, popping off all
    /// events before and between them.
    ///
    /// Panics if the command failed or could not be found in the events.
    pub(crate) fn get_successful_command_execution(
        &self,
        command_name: &str,
    ) -> (CommandStartedEvent, CommandSucceededEvent) {
        let mut command_events = self.handler.command_events.write().unwrap();

        let mut started: Option<CommandStartedEvent> = None;

        while let Some((event, _)) = command_events.pop_front() {
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
    pub(crate) fn get_command_started_events(
        &self,
        command_names: &[&str],
    ) -> Vec<CommandStartedEvent> {
        self.handler.get_command_started_events(command_names)
    }

    /// Gets all command started events, excluding configureFailPoint events.
    pub(crate) fn get_all_command_started_events(&self) -> Vec<CommandStartedEvent> {
        self.handler.get_all_command_started_events()
    }

    pub(crate) fn get_command_events(&self, command_names: &[&str]) -> Vec<CommandEvent> {
        self.handler
            .command_events
            .write()
            .unwrap()
            .drain(..)
            .map(|(event, _)| event)
            .filter(|event| command_names.contains(&event.command_name()))
            .collect()
    }

    pub(crate) fn count_pool_cleared_events(&self) -> usize {
        let mut out = 0;
        for (event, _) in self.handler.cmap_events.read().unwrap().iter() {
            if matches!(event, CmapEvent::PoolCleared(_)) {
                out += 1;
            }
        }
        out
    }

    #[allow(dead_code)]
    pub(crate) fn subscribe_to_events(&self) -> EventSubscriber<'_, EventHandler, Event> {
        self.handler.subscribe()
    }

    pub(crate) fn clear_cached_events(&self) {
        self.handler.clear_cached_events()
    }

    #[allow(dead_code)]
    pub(crate) fn into_client(self) -> crate::Client {
        self.client.into_client()
    }
}

#[tokio::test]
async fn command_started_event_count() {
    let client = EventClient::new().await;
    let coll = client.database("foo").collection("bar");

    for i in 0..10 {
        coll.insert_one(doc! { "x": i }).await.unwrap();
    }

    assert_eq!(client.get_command_started_events(&["insert"]).len(), 10);
}
