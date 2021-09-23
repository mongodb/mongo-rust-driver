use crate::{
    bson::Document,
    event::cmap::{ConnectionCheckoutFailedReason, ConnectionClosedReason},
    test::{CmapEvent, CommandEvent, Event},
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
                has_service_id: Some(event.service_id.is_some()),
            },
            CommandEvent::Failed(event) => ExpectedCommandEvent::Failed {
                command_name: Some(event.command_name),
                has_service_id: Some(event.service_id.is_some()),
            },
            CommandEvent::Succeeded(event) => ExpectedCommandEvent::Succeeded {
                command_name: Some(event.command_name),
                reply: Some(event.reply),
                has_service_id: Some(event.service_id.is_some()),
            },
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub enum ExpectedCmapEvent {
    #[serde(rename = "poolCreatedEvent")]
    PoolCreated {},
    #[serde(rename = "poolReadyEvent")]
    PoolReady {},
    #[serde(rename = "poolClearedEvent", rename_all = "camelCase")]
    PoolCleared { has_service_id: Option<bool> },
    #[serde(rename = "poolClosedEvent")]
    PoolClosed {},
    #[serde(rename = "connectionCreatedEvent")]
    ConnectionCreated {},
    #[serde(rename = "connectionReadyEvent")]
    ConnectionReady {},
    #[serde(rename = "connectionClosedEvent", rename_all = "camelCase")]
    ConnectionClosed {
        reason: Option<ConnectionClosedReason>,
    },
    #[serde(rename = "connectionCheckOutStartedEvent")]
    ConnectionCheckOutStarted {},
    #[serde(rename = "connectionCheckOutFaildEvent", rename_all = "camelCase")]
    ConnectionCheckOutFailed {
        reason: Option<ConnectionCheckoutFailedReason>,
    },
    #[serde(rename = "connectionCheckedOutEvent")]
    ConnectionCheckedOut {},
    #[serde(rename = "connectionCheckedInEvent")]
    ConnectionCheckedIn {},
}

impl From<CmapEvent> for ExpectedCmapEvent {
    fn from(event: CmapEvent) -> Self {
        match event {
            CmapEvent::PoolCreated(_) => Self::PoolCreated {},
            CmapEvent::PoolClosed(_) => Self::PoolClosed {},
            CmapEvent::PoolReady(_) => Self::PoolReady {},
            CmapEvent::ConnectionCreated(_) => Self::ConnectionCreated {},
            CmapEvent::ConnectionReady(_) => Self::ConnectionReady {},
            CmapEvent::ConnectionClosed(ev) => Self::ConnectionClosed {
                reason: Some(ev.reason),
            },
            CmapEvent::ConnectionCheckOutStarted(_) => Self::ConnectionCheckOutStarted {},
            CmapEvent::ConnectionCheckOutFailed(ev) => Self::ConnectionCheckOutFailed {
                reason: Some(ev.reason),
            },
            CmapEvent::ConnectionCheckedOut(_) => Self::ConnectionCheckedOut {},
            CmapEvent::PoolCleared(ev) => Self::PoolCleared {
                has_service_id: Some(ev.service_id.is_some()),
            },
            CmapEvent::ConnectionCheckedIn(_) => Self::ConnectionCheckedIn {},
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug, Deserialize)]
pub enum ObserveEvent {
    #[serde(rename = "commandStartedEvent")]
    CommandStarted,
    #[serde(rename = "commandSucceededEvent")]
    CommandSucceeded,
    #[serde(rename = "commandFailedEvent")]
    CommandFailed,
    #[serde(rename = "poolCreatedEvent")]
    PoolCreated,
    #[serde(rename = "poolReadyEvent")]
    PoolReady,
    #[serde(rename = "poolClearedEvent")]
    PoolCleared,
    #[serde(rename = "poolClosedEvent")]
    PoolClosed,
    #[serde(rename = "connectionCreatedEvent")]
    ConnectionCreated,
    #[serde(rename = "connectionReadyEvent")]
    ConnectionReady,
    #[serde(rename = "connectionClosedEvent")]
    ConnectionClosed,
    #[serde(rename = "connectionCheckOutStartedEvent")]
    ConnectionCheckOutStarted,
    #[serde(rename = "connectionCheckOutFailedEvent")]
    ConnectionCheckOutFailed,
    #[serde(rename = "connectionCheckedOutEvent")]
    ConnectionCheckedOut,
    #[serde(rename = "connectionCheckedInEvent")]
    ConnectionCheckedIn,
}

impl ObserveEvent {
    pub fn matches(&self, event: &Event) -> bool {
        #[allow(clippy::match_like_matches_macro)]
        match (self, event) {
            (Self::CommandStarted, Event::Command(CommandEvent::Started(_))) => true,
            (Self::CommandSucceeded, Event::Command(CommandEvent::Succeeded(_))) => true,
            (Self::CommandFailed, Event::Command(CommandEvent::Failed(_))) => true,
            (Self::PoolCreated, Event::Cmap(CmapEvent::PoolCreated(_))) => true,
            (Self::PoolReady, Event::Cmap(CmapEvent::PoolReady(_))) => true,
            (Self::PoolCleared, Event::Cmap(CmapEvent::PoolCleared(_))) => true,
            (Self::PoolClosed, Event::Cmap(CmapEvent::PoolClosed(_))) => true,
            (Self::ConnectionCreated, Event::Cmap(CmapEvent::ConnectionCreated(_))) => true,
            (Self::ConnectionReady, Event::Cmap(CmapEvent::ConnectionReady(_))) => true,
            (Self::ConnectionClosed, Event::Cmap(CmapEvent::ConnectionClosed(_))) => true,
            (
                Self::ConnectionCheckOutStarted,
                Event::Cmap(CmapEvent::ConnectionCheckOutStarted(_)),
            ) => true,
            (
                Self::ConnectionCheckOutFailed,
                Event::Cmap(CmapEvent::ConnectionCheckOutFailed(_)),
            ) => true,
            (Self::ConnectionCheckedOut, Event::Cmap(CmapEvent::ConnectionCheckedOut(_))) => true,
            (Self::ConnectionCheckedIn, Event::Cmap(CmapEvent::ConnectionCheckedIn(_))) => true,
            _ => false,
        }
    }
}
