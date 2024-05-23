use derive_more::From;
use serde::Serialize;

use super::{event_buffer::EventBuffer, TestClient, TestClientBuilder};
use crate::{
    bson::doc,
    event::{
        cmap::CmapEvent,
        command::{CommandEvent, CommandStartedEvent, CommandSucceededEvent},
        sdam::SdamEvent,
    },
    test::get_client_options,
    Client,
};

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

    pub(crate) fn as_command_event(&self) -> Option<&CommandEvent> {
        if let Event::Command(e) = self {
            Some(e)
        } else {
            None
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
    pub(crate) events: EventBuffer,
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
    pub(crate) fn monitor_events(self) -> EventClientBuilder {
        EventClientBuilder {
            inner: self,
            retain_startup: false,
        }
    }
}

pub(crate) struct EventClientBuilder {
    inner: TestClientBuilder,
    retain_startup: bool,
}

#[allow(deprecated)]
impl EventClientBuilder {
    pub(crate) fn retain_startup_events(mut self) -> Self {
        self.retain_startup = true;
        self
    }

    pub(crate) async fn build(self) -> EventClient {
        let mut inner = self.inner;
        let mut options = match inner.options.take() {
            Some(options) => options,
            None => get_client_options().await.clone(),
        };
        let mut events = EventBuffer::new();
        events.register(&mut options);
        inner.options = Some(options);

        let client = inner.build().await;

        if !self.retain_startup {
            // clear events from commands used to set up client.
            events.retain(|ev| !matches!(ev, Event::Command(_)));
        }

        EventClient { client, events }
    }
}

impl EventClient {
    #[allow(dead_code)]
    pub(crate) fn into_client(self) -> crate::Client {
        self.client.into_client()
    }
}

#[tokio::test]
async fn command_started_event_count() {
    let client = Client::test_builder().monitor_events().build().await;
    let coll = client.database("foo").collection("bar");

    for i in 0..10 {
        coll.insert_one(doc! { "x": i }).await.unwrap();
    }

    assert_eq!(
        client.events.get_command_started_events(&["insert"]).len(),
        10
    );
}
