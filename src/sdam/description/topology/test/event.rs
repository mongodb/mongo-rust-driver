use serde::Deserialize;

use crate::{
    client::options::ServerAddress,
    event::sdam::{
        ServerClosedEvent,
        ServerDescriptionChangedEvent,
        ServerOpeningEvent,
        TopologyClosedEvent,
        TopologyDescriptionChangedEvent,
        TopologyOpeningEvent,
    },
    sdam::{ServerDescription, ServerType, TopologyDescription},
    test::SdamEvent,
};

#[derive(Debug, Deserialize)]
pub enum TestSdamEvent {
    #[serde(rename = "server_description_changed_event")]
    ServerDescriptionChanged(Box<TestServerDescriptionChangedEvent>),
    #[serde(rename = "server_opening_event")]
    ServerOpening(ServerOpeningEvent),
    #[serde(rename = "server_closed_event")]
    ServerClosed(ServerClosedEvent),
    #[serde(rename = "topology_description_changed_event")]
    TopologyDescriptionChanged(TestTopologyDescriptionChangedEvent),
    #[serde(rename = "topology_opening_event")]
    TopologyOpening(TopologyOpeningEvent),
    #[serde(rename = "topology_closed_event")]
    TopologyClosed(TopologyClosedEvent),
}

impl PartialEq<TestSdamEvent> for SdamEvent {
    fn eq(&self, other: &TestSdamEvent) -> bool {
        match (self, other) {
            (Self::ServerDescriptionChanged(s), TestSdamEvent::ServerDescriptionChanged(o)) => {
                s.as_ref() == o.as_ref()
            }
            (Self::ServerOpening(s), TestSdamEvent::ServerOpening(o)) => s.address == o.address,
            (Self::ServerClosed(s), TestSdamEvent::ServerClosed(o)) => s.address == o.address,
            (Self::TopologyDescriptionChanged(s), TestSdamEvent::TopologyDescriptionChanged(o)) => {
                s.as_ref() == o
            }
            (Self::TopologyOpening(_), TestSdamEvent::TopologyOpening(_)) => true,
            (Self::TopologyClosed(_), TestSdamEvent::TopologyClosed(_)) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TestServerDescription {
    address: ServerAddress,
    arbiters: Vec<String>,
    hosts: Vec<String>,
    passives: Vec<String>,
    primary: Option<String>,
    set_name: Option<String>,
    #[serde(rename = "type")]
    server_type: ServerType,
}

impl PartialEq<TestServerDescription> for ServerDescription {
    fn eq(&self, other: &TestServerDescription) -> bool {
        match &self.reply.as_ref().unwrap().as_ref() {
            Some(is_master_reply) => {
                let is_master = &is_master_reply.command_response;
                self.address == other.address
                    && lists_eq(&is_master.arbiters, &other.arbiters)
                    && lists_eq(&is_master.hosts, &other.hosts)
                    && lists_eq(&is_master.passives, &other.passives)
                    && is_master.set_name == other.set_name
                    && is_master.primary == other.primary
                    && is_master.server_type() == other.server_type
            }
            None => {
                self.address == other.address
                    && other.arbiters.is_empty()
                    && other.hosts.is_empty()
                    && other.passives.is_empty()
                    && other.primary.is_none()
                    && other.set_name.is_none()
                    && other.server_type == self.server_type
            }
        }
    }
}

fn lists_eq(actual: &Option<Vec<String>>, expected: &[String]) -> bool {
    if let Some(actual) = actual {
        actual.as_slice() == expected
    } else {
        expected.is_empty()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TestTopologyDescription {
    topology_type: String,
    set_name: Option<String>,
    servers: Vec<TestServerDescription>,
}

impl PartialEq<TestTopologyDescription> for TopologyDescription {
    fn eq(&self, other: &TestTopologyDescription) -> bool {
        if self.topology_type.as_str() != other.topology_type.as_str()
            || self.set_name != other.set_name
        {
            return false;
        }

        if self.servers.len() != other.servers.len() {
            return false;
        }

        for test_server_description in &other.servers {
            if let Some(server_description) = self.servers.get(&test_server_description.address) {
                if server_description != test_server_description {
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestServerDescriptionChangedEvent {
    address: ServerAddress,
    previous_description: TestServerDescription,
    new_description: TestServerDescription,
}

impl PartialEq<TestServerDescriptionChangedEvent> for ServerDescriptionChangedEvent {
    fn eq(&self, other: &TestServerDescriptionChangedEvent) -> bool {
        self.address == other.address
            && self.previous_description.description.as_ref() == &other.previous_description
            && self.new_description.description.as_ref() == &other.new_description
    }
}

impl PartialEq<TestTopologyDescriptionChangedEvent> for TopologyDescriptionChangedEvent {
    fn eq(&self, other: &TestTopologyDescriptionChangedEvent) -> bool {
        self.previous_description.description == other.previous_description
            && self.new_description.description == other.new_description
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestTopologyDescriptionChangedEvent {
    previous_description: TestTopologyDescription,
    new_description: TestTopologyDescription,
}
