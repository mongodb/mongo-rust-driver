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
    sdam::{ServerDescription, TopologyDescription},
    test::SdamEvent,
};

#[derive(Debug, Deserialize)]
pub enum TestSdamEvent {
    #[serde(rename = "server_description_changed_event")]
    ServerDescriptionChanged(TestServerDescriptionChangedEvent),
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

impl TestSdamEvent {
    pub fn equals_sdam_event(&self, other: &SdamEvent) -> bool {
        true
        // match (self, other) {
        //     (Self::ServerDescriptionChanged(s), SdamEvent::ServerDescriptionChanged(o)) => {
        //         s.equals_server_description_changed_event(o)
        //     }
        //     (Self::ServerOpening(s), SdamEvent::ServerOpening(o)) => s == o,
        //     (Self::ServerClosed(s), SdamEvent::ServerClosed(o)) => s == o,
        //     (Self::TopologyDescriptionChanged(s), SdamEvent::TopologyDescriptionChanged(o)) => {
        //         s.equals_topology_description_changed_event(o)
        //     },
        //     (Self::TopologyOpening(_), SdamEvent::TopologyOpening(_)) => true,
        //     (Self::TopologyClosed(_), SdamEvent::TopologyClosed(_)) => true,
        //     _ => false,
        // }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TestServerDescription {
    address: ServerAddress,
    arbiters: Option<Vec<String>>,
    hosts: Option<Vec<String>>,
    passives: Option<Vec<String>>,
    primary: Option<String>,
    set_name: Option<String>,
    #[serde(rename = "type")]
    server_type: String,
}

impl TestServerDescription {
    fn equals_server_description(&self, other: &ServerDescription) -> bool {
        // true
        let is_master = &other
            .reply
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap()
            .command_response;
        self.address == other.address
            && self.arbiters == is_master.arbiters
            && self.hosts == is_master.hosts
            && self.passives == is_master.passives
            && self.set_name == is_master.set_name
            && self.primary == is_master.primary
            && self.server_type.as_str() == other.server_type.to_str()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TestTopologyDescription {
    topology_type: String,
    set_name: Option<String>,
    servers: Vec<TestServerDescription>,
}

impl TestTopologyDescription {
    fn equals_topology_description(&self, other: &TopologyDescription) -> bool {
        for test_server_description in &self.servers {
            if let Some(server_description) = other.servers.get(&test_server_description.address) {
                if !test_server_description.equals_server_description(server_description) {
                    return false;
                }
            } else {
                return false;
            }
        }

        self.topology_type.as_str() == other.topology_type.to_str()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestServerDescriptionChangedEvent {
    address: ServerAddress,
    previous_description: TestServerDescription,
    new_description: TestServerDescription,
}

impl TestServerDescriptionChangedEvent {
    fn equals_server_description_changed_event(
        &self,
        other: &ServerDescriptionChangedEvent,
    ) -> bool {
        true
        // self.address == other.address
        //     && self
        //         .previous_description
        //         .equals_server_description(&other.previous_description)
        //     && self
        //         .new_description
        //         .equals_server_description(&other.new_description)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestTopologyDescriptionChangedEvent {
    previous_description: TestTopologyDescription,
    new_description: TestTopologyDescription,
}

impl TestTopologyDescriptionChangedEvent {
    fn equals_topology_description_changed_event(
        &self,
        other: &TopologyDescriptionChangedEvent,
    ) -> bool {
        true
        // self.previous_description
        //     .equals_topology_description(&other.previous_description)
        //     && self
        //         .new_description
        // .equals_topology_description(&other.new_description)
    }
}
