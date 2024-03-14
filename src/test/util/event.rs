use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
    time::Duration,
};

use derive_more::From;
use serde::Serialize;
use time::OffsetDateTime;

use super::{TestClient, TestClientBuilder};
use crate::{
    bson::doc, event::{
        cmap::CmapEvent,
        command::{CommandEvent, CommandStartedEvent, CommandSucceededEvent},
        sdam::SdamEvent,
    }, options::ClientOptions, Client
};

pub(crate) use super::handler::EventHandler;

pub(crate) type EventQueue<T> = Arc<RwLock<VecDeque<(T, OffsetDateTime)>>>;

fn add_event_to_queue<T>(event_queue: &EventQueue<T>, event: T) {
    event_queue
        .write()
        .unwrap()
        .push_back((event, OffsetDateTime::now_utc()))
}

#[derive(Clone, Debug, From, Serialize)]
#[serde(untagged)]
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

    pub(crate) fn request_id(&self) -> i32 {
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
pub(crate) struct EventClient {
    client: TestClient,
    pub(crate) handler: EventHandler,
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
            inner = inner.event_handler(EventHandler::new());
        }
        let mut handler = inner.handler().unwrap().clone();
        let client = inner.build().await;

        // clear events from commands used to set up client.
        handler.retain(|ev| !matches!(ev, Event::Command(_)));

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
            .event_handler(handler)
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
            .event_handler(event_handler)
            .event_client()
            .build()
            .await
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

    assert_eq!(client.handler.get_command_started_events(&["insert"]).len(), 10);
}