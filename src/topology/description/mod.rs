#[cfg(test)]
mod test;

use std::collections::HashMap;

use bson::oid::ObjectId;

use crate::{
    options::ClientOptions,
    read_preference::ReadPreference,
    topology::server::{ServerDescription, ServerType},
};

const MAX_WIRE_VERSION: i32 = 6;
const MIN_WIRE_VERSION: i32 = 4;
const MIN_MONGODB_VERSION: &str = "3.2";

#[derive(Debug, Clone, Copy, Deserialize, PartialEq)]
pub enum TopologyType {
    Single,
    ReplicaSetNoPrimary,
    ReplicaSetWithPrimary,
    Sharded,
    Unknown,
}

impl TopologyType {
    pub fn is_replica_set(self) -> bool {
        match self {
            TopologyType::ReplicaSetNoPrimary | TopologyType::ReplicaSetWithPrimary => true,
            _ => false,
        }
    }
}

impl Default for TopologyType {
    fn default() -> Self {
        TopologyType::Unknown
    }
}

enum HelperResult {
    Ok,
    NeedToRemove,
    Stale,
    Reset(Vec<String>),
}

#[derive(Debug, Default, Clone)]
pub struct TopologyDescription {
    pub topology_type: TopologyType,
    pub set_name: Option<String>,
    pub max_set_version: Option<i32>,
    pub max_election_id: Option<ObjectId>,
    pub compatibility_error: Option<String>,
    server_descriptions: HashMap<String, ServerDescription>,
    logical_session_timeout_minutes: Option<i32>,
}

impl TopologyDescription {
    pub fn new(conn_string: ClientOptions) -> Self {
        let topology_type = if conn_string.repl_set_name.is_some() {
            TopologyType::ReplicaSetNoPrimary
        } else if conn_string.hosts.len() == 1 {
            TopologyType::Single
        } else {
            TopologyType::Unknown
        };

        Self {
            topology_type,
            set_name: conn_string.repl_set_name,
            server_descriptions: conn_string
                .hosts
                .into_iter()
                .map(|host| {
                    (
                        host.display(),
                        ServerDescription::new(&host.display(), None),
                    )
                })
                .collect(),
            ..Default::default()
        }
    }

    pub(crate) fn slave_ok(&self, address: &str, read_preference: Option<&ReadPreference>) -> bool {
        match self.topology_type {
            TopologyType::Single if self.get_server_type(address) != Some(ServerType::Mongos) => {
                true
            }
            _ => match read_preference {
                Some(rp) => !rp.is_primary(),

                // The default read preference is primary, so if none is specified, we don't set
                // slaveOk.
                None => false,
            },
        }
    }

    pub(crate) fn server_descriptions(&self) -> HashMap<String, ServerDescription> {
        self.server_descriptions.clone()
    }

    pub(crate) fn server_addresses(&self) -> impl Iterator<Item = &String> {
        self.server_descriptions.keys()
    }

    pub(crate) fn contains_server(&self, address: &str) -> bool {
        self.server_descriptions.contains_key(address)
    }

    pub(crate) fn remove_server(&mut self, address: &str) {
        self.server_descriptions.remove(address);
    }

    pub(crate) fn get_server_type(&self, address: &str) -> Option<ServerType> {
        self.server_descriptions
            .get(address)
            .map(|description| description.server_type)
    }

    pub(crate) fn update(&mut self, mut server_description: ServerDescription) {
        match self.server_descriptions.get(&server_description.address) {
            Some(old_server_description) => {
                server_description.update_round_trip_time(old_server_description.round_trip_time)
            }
            None => return,
        };

        let address = server_description.address.clone();
        let server_max_wire_version = server_description.max_wire_version;
        let server_min_wire_version = server_description.min_wire_version;
        let server_type = server_description.server_type;

        self.server_descriptions
            .insert(server_description.address.clone(), server_description);

        self.compatibility_error = None;

        if server_min_wire_version > MAX_WIRE_VERSION {
            self.compatibility_error = Some(format!(
                "Server at {} requires wire version {}, but this version of the Rust driver only \
                 supports up to {}",
                address, server_min_wire_version, MAX_WIRE_VERSION,
            ));
        }

        if server_max_wire_version < MIN_WIRE_VERSION {
            self.compatibility_error = Some(format!(
                "Server at {} reports wire version {}, but this version of the Rust driver \
                 requires at least {} (MongoDB {})",
                address, server_max_wire_version, MIN_WIRE_VERSION, MIN_MONGODB_VERSION,
            ))
        }

        #[cfg_attr(feature = "clippy", allow(match_same_arms))]
        match (self.topology_type, server_type) {
            (TopologyType::Single, _) => {}
            (TopologyType::Unknown, ServerType::Unknown) => {}
            (TopologyType::Unknown, ServerType::Standalone) => {
                self.update_unknown_with_standalone(&address);
            }
            (TopologyType::Unknown, ServerType::Mongos) => {
                self.topology_type = TopologyType::Sharded;
            }
            (TopologyType::Unknown, ServerType::RSPrimary) => {
                self.update_rs_from_primary(&address);
            }
            (TopologyType::Unknown, ServerType::RSGhost) => {}
            (TopologyType::Unknown, _) => {
                self.topology_type = TopologyType::ReplicaSetNoPrimary;
                self.update_rs_without_primary(&address);
            }
            (TopologyType::Sharded, ServerType::Unknown)
            | (TopologyType::Sharded, ServerType::Mongos) => {}
            (TopologyType::Sharded, _) => {
                self.remove_server(&address);
            }
            (TopologyType::ReplicaSetNoPrimary, ServerType::Unknown)
            | (TopologyType::ReplicaSetNoPrimary, ServerType::RSGhost) => {}
            (TopologyType::ReplicaSetNoPrimary, ServerType::Standalone)
            | (TopologyType::ReplicaSetNoPrimary, ServerType::Mongos) => {
                self.remove_server(&address);
            }
            (TopologyType::ReplicaSetNoPrimary, ServerType::RSPrimary) => {
                self.update_rs_from_primary(&address);
            }
            (TopologyType::ReplicaSetNoPrimary, _) => {
                self.update_rs_without_primary(&address);
            }
            (TopologyType::ReplicaSetWithPrimary, ServerType::Unknown)
            | (TopologyType::ReplicaSetWithPrimary, ServerType::RSGhost) => {
                self.check_if_has_primary()
            }
            (TopologyType::ReplicaSetWithPrimary, ServerType::Standalone)
            | (TopologyType::ReplicaSetWithPrimary, ServerType::Mongos) => {
                self.remove_server(&address);
                self.check_if_has_primary();
            }
            (TopologyType::ReplicaSetWithPrimary, ServerType::RSPrimary) => {
                self.update_rs_from_primary(&address);
            }
            (TopologyType::ReplicaSetWithPrimary, _) => {
                self.update_rs_with_primary_from_member(&address);
            }
        };
    }

    fn check_if_has_primary(&mut self) {
        let has_primary = self
            .server_descriptions
            .values()
            .any(|server| server.server_type == ServerType::RSPrimary);

        self.topology_type = if has_primary {
            TopologyType::ReplicaSetWithPrimary
        } else {
            TopologyType::ReplicaSetNoPrimary
        };
    }

    fn update_unknown_with_standalone(&mut self, address: &str) {
        // If only a single seed was provided, then the TopologyType would already be
        // single, so we simply remove the server from the topology.
        self.remove_server(address);
    }

    fn update_rs_from_primary(&mut self, address: &str) {
        match self.update_rs_from_primary_helper(address) {
            HelperResult::Ok => {}
            HelperResult::NeedToRemove => {
                self.remove_server(address);
            }
            HelperResult::Stale => {
                self.server_descriptions
                    .insert(address.to_string(), ServerDescription::new(address, None));
            }
            HelperResult::Reset(mut new_hosts) => {
                let old_primaries: Vec<String> = self
                    .server_descriptions
                    .iter()
                    .filter(|&(_, server)| {
                        server.address != address && server.server_type == ServerType::RSPrimary
                    })
                    .map(|(address, _)| address.to_string())
                    .collect();

                for address in old_primaries {
                    let description = ServerDescription::new(&address, None);
                    self.server_descriptions.insert(address, description);
                }

                self.server_descriptions
                    .retain(|address, _| new_hosts.contains(address));

                new_hosts.retain(|address| !self.server_descriptions.contains_key(address));

                for address in new_hosts {
                    let server = ServerDescription::new(&address, None);
                    self.server_descriptions.insert(address, server);
                }
            }
        };

        self.check_if_has_primary();
    }

    fn update_rs_from_primary_helper(&mut self, address: &str) -> HelperResult {
        let server_description = match self.server_descriptions.get_mut(address) {
            Some(description) => description,
            None => return HelperResult::Ok,
        };

        if self.set_name.is_none() {
            self.set_name = server_description.set_name.clone();
        } else if self.set_name.as_ref() != server_description.set_name.as_ref() {
            return HelperResult::NeedToRemove;
        }

        if let Some(description_set_version) = server_description.set_version {
            if let Some(ref description_election_id) = server_description.election_id {
                if let Some(topology_max_set_version) = self.max_set_version {
                    if let Some(ref topology_max_election_id) = self.max_election_id {
                        if topology_max_set_version > description_set_version
                            || (topology_max_set_version == description_set_version
                                && topology_max_election_id > description_election_id)
                        {
                            return HelperResult::Stale;
                        }
                    }
                }

                self.max_election_id = server_description.election_id.clone();
            }
        }

        if let Some(description_set_version) = server_description.set_version {
            if self
                .max_set_version
                .map(|v| description_set_version > v)
                .unwrap_or(true)
            {
                self.max_set_version = Some(description_set_version);
            }
        }

        HelperResult::Reset(
            server_description
                .hosts
                .iter()
                .chain(server_description.passives.iter())
                .chain(server_description.arbiters.iter())
                .cloned()
                .collect(),
        )
    }

    fn update_rs_with_primary_from_member(&mut self, address: &str) {
        let (need_to_remove, check_for_primary) = match self.server_descriptions.get(address) {
            Some(description) => self.update_rs_with_primary_from_member_helper(description),
            None => return,
        };

        if need_to_remove {
            self.remove_server(address);
        }

        if check_for_primary {
            self.check_if_has_primary();
            return;
        }

        if !self
            .server_descriptions
            .values()
            .any(|server| server.server_type == ServerType::RSPrimary)
        {
            self.topology_type = TopologyType::ReplicaSetNoPrimary;
        }
    }

    fn update_rs_with_primary_from_member_helper(
        &self,
        server_description: &ServerDescription,
    ) -> (bool, bool) {
        let need_to_remove = self.set_name.as_ref() != server_description.set_name.as_ref();

        if let Some(ref me) = server_description.me {
            if &server_description.address != me {
                return (true, true);
            }
        }

        (need_to_remove, false)
    }

    fn update_rs_without_primary(&mut self, address: &str) {
        let (new_server_addresses, need_to_remove) = self.update_rs_without_primary_helper(address);

        if let Some(addresses) = new_server_addresses {
            for address in addresses {
                let description = ServerDescription::new(&address, None);

                self.server_descriptions.insert(address, description);
            }
        }

        if need_to_remove {
            self.remove_server(address);
        }
    }

    fn update_rs_without_primary_helper(&mut self, address: &str) -> (Option<Vec<String>>, bool) {
        let server_description = match self.server_descriptions.get(address) {
            Some(description) => description,
            None => return (None, false),
        };

        if self.set_name.is_none() {
            self.set_name = server_description.set_name.clone();
        } else if self.set_name != server_description.set_name {
            return (None, true);
        }

        let new_hosts: Vec<String> = server_description
            .hosts
            .iter()
            .chain(server_description.passives.iter())
            .chain(server_description.arbiters.iter())
            .filter(|address| !self.server_descriptions.contains_key(&address[..]))
            .cloned()
            .collect();

        let need_to_remove = match server_description.me {
            Some(ref me) => &server_description.address != me,
            None => false,
        };

        (Some(new_hosts), need_to_remove)
    }
}
