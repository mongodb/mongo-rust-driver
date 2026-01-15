use crate::{
    bson::Document,
    event::{
        cmap::{CmapEvent, ConnectionCheckoutFailedReason, ConnectionClosedReason},
        command::CommandEvent,
        sdam::SdamEvent,
    },
    test::Event,
    ServerType,
    TopologyType,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(untagged, deny_unknown_fields, rename_all = "camelCase")]
pub(crate) enum ExpectedEvent {
    Cmap(ExpectedCmapEvent),
    Command(ExpectedCommandEvent),
    Sdam(Box<ExpectedSdamEvent>),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum ExpectedCommandEvent {
    #[serde(rename = "commandStartedEvent", rename_all = "camelCase")]
    Started {
        command_name: Option<String>,
        database_name: Option<String>,
        command: Option<Document>,
        has_service_id: Option<bool>,
        has_server_connection_id: Option<bool>,
    },
    #[serde(rename = "commandSucceededEvent", rename_all = "camelCase")]
    Succeeded {
        command_name: Option<String>,
        reply: Option<Document>,
        has_service_id: Option<bool>,
        has_server_connection_id: Option<bool>,
    },
    #[serde(rename = "commandFailedEvent", rename_all = "camelCase")]
    Failed {
        command_name: Option<String>,
        has_service_id: Option<bool>,
        has_server_connection_id: Option<bool>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum ExpectedCmapEvent {
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
    #[serde(rename = "connectionCheckOutFailedEvent", rename_all = "camelCase")]
    ConnectionCheckOutFailed {
        reason: Option<ConnectionCheckoutFailedReason>,
    },
    #[serde(rename = "connectionCheckedOutEvent")]
    ConnectionCheckedOut {},
    #[serde(rename = "connectionCheckedInEvent")]
    ConnectionCheckedIn {},
}

#[derive(Debug, Deserialize)]
pub(crate) enum ExpectedSdamEvent {
    #[serde(rename = "serverDescriptionChangedEvent", rename_all = "camelCase")]
    ServerDescriptionChanged {
        #[allow(unused)]
        previous_description: Option<TestServerDescription>,
        new_description: Option<TestServerDescription>,
    },
    #[serde(rename = "topologyOpeningEvent")]
    TopologyOpening {},
    #[serde(rename = "topologyClosedEvent")]
    TopologyClosed {},
    #[serde(rename = "topologyDescriptionChangedEvent", rename_all = "camelCase")]
    TopologyDescriptionChanged {
        previous_description: Option<TestTopologyDescription>,
        new_description: Option<TestTopologyDescription>,
    },
    #[serde(rename = "serverHeartbeatStartedEvent", rename_all = "camelCase")]
    ServerHeartbeatStarted { awaited: Option<bool> },
    #[serde(rename = "serverHeartbeatSucceededEvent", rename_all = "camelCase")]
    ServerHeartbeatSucceeded { awaited: Option<bool> },
    #[serde(rename = "serverHeartbeatFailedEvent", rename_all = "camelCase")]
    ServerHeartbeatFailed { awaited: Option<bool> },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TestServerDescription {
    #[serde(rename = "type")]
    pub(crate) server_type: Option<ServerType>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TestTopologyDescription {
    #[serde(rename = "type")]
    pub(crate) topology_type: Option<TopologyType>,
}

#[derive(Copy, Clone, Debug, Deserialize)]
pub(crate) enum ObserveEvent {
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
    #[serde(rename = "serverDescriptionChangedEvent")]
    ServerDescriptionChanged,
    #[serde(rename = "topologyOpeningEvent")]
    TopologyOpening,
    #[serde(rename = "topologyClosedEvent")]
    TopologyClosed,
    #[serde(rename = "topologyDescriptionChangedEvent")]
    TopologyDescriptionChanged,
    #[serde(rename = "serverHeartbeatStartedEvent")]
    ServerHeartbeatStarted,
    #[serde(rename = "serverHeartbeatSucceededEvent")]
    ServerHeartbeatSucceeded,
    #[serde(rename = "serverHeartbeatFailedEvent")]
    ServerHeartbeatFailed,
}

impl ObserveEvent {
    pub(crate) fn matches(&self, event: &Event) -> bool {
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
                Event::Cmap(CmapEvent::ConnectionCheckoutStarted(_)),
            ) => true,
            (
                Self::ConnectionCheckOutFailed,
                Event::Cmap(CmapEvent::ConnectionCheckoutFailed(_)),
            ) => true,
            (Self::ConnectionCheckedOut, Event::Cmap(CmapEvent::ConnectionCheckedOut(_))) => true,
            (Self::ConnectionCheckedIn, Event::Cmap(CmapEvent::ConnectionCheckedIn(_))) => true,
            (Self::TopologyOpening, Event::Sdam(SdamEvent::TopologyOpening(_))) => true,
            (Self::TopologyClosed, Event::Sdam(SdamEvent::TopologyClosed(_))) => true,
            (
                Self::TopologyDescriptionChanged,
                Event::Sdam(SdamEvent::TopologyDescriptionChanged(_)),
            ) => true,
            (Self::ServerHeartbeatStarted, Event::Sdam(SdamEvent::ServerHeartbeatStarted(_))) => {
                true
            }
            (
                Self::ServerHeartbeatSucceeded,
                Event::Sdam(SdamEvent::ServerHeartbeatSucceeded(_)),
            ) => true,
            (Self::ServerHeartbeatFailed, Event::Sdam(SdamEvent::ServerHeartbeatFailed(_))) => true,
            (
                Self::ServerDescriptionChanged,
                Event::Sdam(SdamEvent::ServerDescriptionChanged(_)),
            ) => true,
            _ => false,
        }
    }
}
