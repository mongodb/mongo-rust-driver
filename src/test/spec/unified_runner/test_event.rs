use crate::{
    bson::Document,
    event::cmap::{ConnectionCheckoutFailedReason, ConnectionClosedReason},
    test::{Event, CommandEvent, CmapEvent},
};
use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(untagged, deny_unknown_fields, rename_all = "camelCase")]
pub enum ExpectedEvent {
    Cmap(ExpectedCmapEvent),
    Command(ExpectedCommandEvent),
    Sdam,
}

impl From<Event> for ExpectedEvent {
    fn from(event: Event) -> Self {
        match event {
            Event::Command(sub) => ExpectedEvent::Command(sub.into()),
            Event::Cmap(sub) => ExpectedEvent::Cmap(sub.into()),
            Event::Sdam(_) => ExpectedEvent::Sdam,
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub enum ExpectedCommandEvent {
    #[serde(rename = "commandStartedEvent", rename_all = "camelCase")]
    Started {
        command_name: Option<String>,
        database_name: Option<String>,
        command: Option<Document>,
        has_service_id: Option<bool>,
    },
    #[serde(rename = "commandSucceededEvent", rename_all = "camelCase")]
    Succeeded {
        command_name: Option<String>,
        reply: Option<Document>,
        has_service_id: Option<bool>,
    },
    #[serde(rename = "commandFailedEvent", rename_all = "camelCase")]
    Failed {
        command_name: Option<String>,
        has_service_id: Option<bool>,
    },
}

impl From<CommandEvent> for ExpectedCommandEvent {
    fn from(event: CommandEvent) -> Self {
        match event {
            CommandEvent::Started(event) => ExpectedCommandEvent::Started {
                command_name: Some(event.command_name),
                database_name: Some(event.db),
                command: Some(event.command),
                // TODO RUST-956 Populate the `has_service_id` field once `service_id` is present.
                has_service_id: None,
            },
            CommandEvent::Failed(event) => ExpectedCommandEvent::Failed {
                command_name: Some(event.command_name),
                // TODO RUST-956 Populate the `has_service_id` field once `service_id` is present.
                has_service_id: None,
            },
            CommandEvent::Succeeded(event) => ExpectedCommandEvent::Succeeded {
                command_name: Some(event.command_name),
                reply: Some(event.reply),
                // TODO RUST-956 Populate the `has_service_id` field once `service_id` is present.
                has_service_id: None,
            },
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum ExpectedCmapEvent {
    #[serde(rename = "poolCreatedEvent")]
    PoolCreated,
    #[serde(rename = "poolReadyEvent")]
    PoolReady,
    #[serde(rename = "poolClearedEvent")]
    PoolCleared {
        has_service_id: Option<bool>,
    },
    #[serde(rename = "poolClosedEvent")]
    PoolClosed,
    #[serde(rename = "connectionCreatedEvent")]
    ConnectionCreated,
    #[serde(rename = "connectionReadyEvent")]
    ConnectionReady,
    #[serde(rename = "connectionClosedEvent")]
    ConnectionClosed {
        reason: Option<ConnectionClosedReason>,
    },
    #[serde(rename = "connectionCheckOutStartedEvent")]
    ConnectionCheckOutStarted,
    #[serde(rename = "connectionCheckOutFaildEvent")]
    ConnectionCheckOutFailed {
        reason: Option<ConnectionCheckoutFailedReason>,
    },
    #[serde(rename = "connectionCheckedOutEvent")]
    ConnectionCheckedOut,
    #[serde(rename = "connectionCheckedInEvent")]
    ConnectionCheckedIn,
}

impl From<CmapEvent> for ExpectedCmapEvent {
    fn from(event: CmapEvent) -> Self {
        match event {
            CmapEvent::PoolCreated(_) => Self::PoolCreated,
            CmapEvent::PoolClosed(_) => Self::PoolClosed,
            CmapEvent::PoolReady(_) => Self::PoolReady,
            CmapEvent::ConnectionCreated(_) => Self::ConnectionCreated,
            CmapEvent::ConnectionReady(_) => Self::ConnectionReady,
            CmapEvent::ConnectionClosed(ev) => Self::ConnectionClosed { reason: Some(ev.reason) },
            CmapEvent::ConnectionCheckOutStarted(_) => Self::ConnectionCheckOutStarted,
            CmapEvent::ConnectionCheckOutFailed(ev) => Self::ConnectionCheckOutFailed { reason: Some(ev.reason) },
            CmapEvent::ConnectionCheckedOut(_) => Self::ConnectionCheckedOut,
            CmapEvent::PoolCleared(_) => {
                // TODO RUST-956 populate `has_service_id`
                Self::PoolCleared { has_service_id: None }
            },
            CmapEvent::ConnectionCheckedIn(_) => Self::ConnectionCheckedIn,
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ObserveEvent {
    CommandStartedEvent,
    CommandSucceededEvent,
    CommandFailedEvent,
    PoolCreatedEvent,
    PoolReadyEvent,
    PoolClearedEvent,
    PoolClosedEvent,
    ConnectionCreatedEvent,
    ConnectionReadyEvent,
    ConnectionClosedEvent,
    ConnectionCheckOutStartedEvent,
    ConnectionCheckOutFailedEvent,
    ConnectionCheckedOutEvent,
    ConnectionCheckedInEvent,
}

impl ObserveEvent {
    pub fn matches(&self, event: &Event) -> bool {
        match (self, event) {
            (Self::CommandStartedEvent, Event::Command(CommandEvent::Started(_))) => true,
            (Self::CommandSucceededEvent, Event::Command(CommandEvent::Succeeded(_))) => true,
            (Self::CommandFailedEvent, Event::Command(CommandEvent::Failed(_))) => true,
            (Self::PoolCreatedEvent, Event::Cmap(CmapEvent::PoolCreated(_))) => true,
            (Self::PoolReadyEvent, Event::Cmap(CmapEvent::PoolReady(_))) => true,
            (Self::PoolClearedEvent, Event::Cmap(CmapEvent::PoolCleared(_))) => true,
            (Self::PoolClosedEvent, Event::Cmap(CmapEvent::PoolClosed(_))) => true,
            (Self::ConnectionCreatedEvent, Event::Cmap(CmapEvent::ConnectionCreated(_))) => true,
            (Self::ConnectionReadyEvent, Event::Cmap(CmapEvent::ConnectionReady(_))) => true,
            (Self::ConnectionClosedEvent, Event::Cmap(CmapEvent::ConnectionClosed(_))) => true,
            (Self::ConnectionCheckOutStartedEvent, Event::Cmap(CmapEvent::ConnectionCheckOutStarted(_))) => true,
            (Self::ConnectionCheckOutFailedEvent, Event::Cmap(CmapEvent::ConnectionCheckOutFailed(_))) => true,
            (Self::ConnectionCheckedOutEvent, Event::Cmap(CmapEvent::ConnectionCheckedOut(_))) => true,
            (Self::ConnectionCheckedInEvent, Event::Cmap(CmapEvent::ConnectionCheckedIn(_))) => true,
            _ => false,
        }
    }
}