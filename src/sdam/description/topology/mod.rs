mod server_selection;
#[cfg(test)]
mod test;

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use bson::oid::ObjectId;
use serde::Deserialize;

use crate::{
    cmap::Command,
    error::{ErrorKind, Result},
    options::{ClientOptions, StreamAddress},
    sdam::description::server::{ServerDescription, ServerType},
    selection_criteria::{ReadPreference, SelectionCriteria},
};

const DEFAULT_HEARTBEAT_FREQUENCY: Duration = Duration::from_secs(10);

/// The TopologyType type, as described by the SDAM spec.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize)]
pub(crate) enum TopologyType {
    Single,
    ReplicaSetNoPrimary,
    ReplicaSetWithPrimary,
    Sharded,
    Unknown,
}

impl Default for TopologyType {
    fn default() -> Self {
        TopologyType::Unknown
    }
}

/// The TopologyDescription type, as described by the SDAM spec.
#[derive(Debug, Clone)]
pub(crate) struct TopologyDescription {
    /// Whether or not the topology was initialized with a single seed.
    single_seed: bool,

    /// The current type of the topology.
    topology_type: TopologyType,

    /// The replica set name of the topology.
    set_name: Option<String>,

    /// The highest replica set version the driver has seen by a member of the topology.
    max_set_version: Option<i32>,

    /// The highest replica set election id the driver has seen by a member of the topology.
    max_election_id: Option<ObjectId>,

    /// Describes the compatibility issue between the driver and server with regards to the
    /// respective supported wire versions.
    compatibility_error: Option<String>,

    // TODO RUST-149: Session support.
    logical_session_timeout_minutes: Option<u32>,

    /// The amount of latency beyond that of the suitable server with the minimum latency that is
    /// acceptable for a read operation.
    local_threshold: Option<Duration>,

    /// The maximum amount of time to wait before checking a given server by sending an isMaster.
    heartbeat_freq: Option<Duration>,

    /// The server descriptions of each member of the topology.
    servers: HashMap<StreamAddress, ServerDescription>,
}

impl PartialEq for TopologyDescription {
    fn eq(&self, other: &Self) -> bool {
        // Since we only use TopologyDescription equality to determine whether to wake up server
        // selection operations to try to select again, the only fields we care about are the ones
        // checked by the server selection algorithm.
        self.compatibility_error == other.compatibility_error
            && self.servers == other.servers
            && self.topology_type == other.topology_type
    }
}

impl TopologyDescription {
    pub(crate) fn new(options: ClientOptions) -> Result<Self> {
        verify_max_staleness(
            options
                .selection_criteria
                .as_ref()
                .and_then(|criteria| criteria.max_staleness()),
        )?;

        let topology_type = if let Some(true) = options.direct_connection {
            TopologyType::Single
        } else if options.repl_set_name.is_some() {
            TopologyType::ReplicaSetNoPrimary
        } else {
            TopologyType::Unknown
        };

        let servers: HashMap<_, _> = options
            .hosts
            .into_iter()
            .map(|address| {
                let description = ServerDescription::new(address.clone(), None);

                (address, description)
            })
            .collect();

        Ok(Self {
            single_seed: servers.len() == 1,
            topology_type,
            set_name: options.repl_set_name,
            max_set_version: None,
            max_election_id: None,
            compatibility_error: None,
            logical_session_timeout_minutes: None,
            local_threshold: options.local_threshold,
            heartbeat_freq: options.heartbeat_freq,
            servers,
        })
    }

    pub(crate) fn topology_type(&self) -> TopologyType {
        self.topology_type
    }

    pub(crate) fn server_addresses(&self) -> impl Iterator<Item = &StreamAddress> {
        self.servers.keys()
    }

    pub(crate) fn get_server_description(
        &self,
        address: &StreamAddress,
    ) -> Option<&ServerDescription> {
        self.servers.get(address)
    }

    pub(crate) fn update_command_with_read_pref(
        &self,
        server_type: ServerType,
        command: &mut Command,
        criteria: Option<&SelectionCriteria>,
    ) {
        match (self.topology_type, server_type) {
            (TopologyType::Sharded, ServerType::Mongos)
            | (TopologyType::Single, ServerType::Mongos) => {
                self.update_command_read_pref_for_mongos(command, criteria);
            }
            (TopologyType::Single, ServerType::Standalone) => {}
            (TopologyType::Single, _) => {
                let specified_read_pref = criteria
                    .and_then(SelectionCriteria::as_read_pref)
                    .map(Clone::clone);

                let resolved_read_pref = match specified_read_pref {
                    Some(ReadPreference::Primary) | None => ReadPreference::PrimaryPreferred {
                        max_staleness: None,
                        tag_sets: None,
                    },
                    Some(other) => other,
                };

                command.read_pref = Some(resolved_read_pref);
            }
            _ => {}
        }
    }

    fn update_command_read_pref_for_mongos(
        &self,
        command: &mut Command,
        criteria: Option<&SelectionCriteria>,
    ) {
        match criteria {
            Some(SelectionCriteria::ReadPreference(ReadPreference::Secondary {
                max_staleness,
                tag_sets,
            })) => {
                command.read_pref = Some(ReadPreference::Secondary {
                    max_staleness: *max_staleness,
                    tag_sets: tag_sets.clone(),
                });
            }
            Some(SelectionCriteria::ReadPreference(ReadPreference::PrimaryPreferred {
                max_staleness,
                tag_sets,
            })) => {
                command.read_pref = Some(ReadPreference::PrimaryPreferred {
                    max_staleness: *max_staleness,
                    tag_sets: tag_sets.clone(),
                });
            }
            Some(SelectionCriteria::ReadPreference(ReadPreference::SecondaryPreferred {
                max_staleness,
                tag_sets,
            })) if max_staleness.is_some() || tag_sets.is_some() => {
                command.read_pref = Some(ReadPreference::SecondaryPreferred {
                    max_staleness: *max_staleness,
                    tag_sets: tag_sets.clone(),
                });
            }
            Some(SelectionCriteria::ReadPreference(ReadPreference::Nearest {
                max_staleness,
                tag_sets,
            })) => {
                command.read_pref = Some(ReadPreference::Nearest {
                    max_staleness: *max_staleness,
                    tag_sets: tag_sets.clone(),
                });
            }
            _ => {}
        }
    }

    /// Gets the heartbeat frequency.
    fn heartbeat_frequency(&self) -> Duration {
        self.heartbeat_freq.unwrap_or(DEFAULT_HEARTBEAT_FREQUENCY)
    }

    /// Check the cluster for a compatibility error, and record the error message if one is found.
    fn check_compatibility(&mut self) {
        self.compatibility_error = None;

        for server in self.servers.values() {
            let error_message = server.compatibility_error_message();

            if error_message.is_some() {
                self.compatibility_error = error_message;
                return;
            }
        }
    }

    pub(crate) fn compatibility_error(&self) -> Option<&String> {
        self.compatibility_error.as_ref()
    }

    /// Update the ServerDescription's round trip time based on the rolling average.
    fn update_round_trip_time(&self, server_description: &mut ServerDescription) {
        if let Some(old_rtt) = self
            .servers
            .get(&server_description.address)
            .and_then(|server_desc| server_desc.average_round_trip_time)
        {
            if let Some(new_rtt) = server_description.average_round_trip_time {
                server_description.average_round_trip_time =
                    Some((new_rtt / 5) + (old_rtt * 4 / 5));
            }
        }
    }

    /// Returns the diff between this topology description and the provided one, or `None` if
    /// they are equal.
    ///
    /// The returned `TopologyDescriptionDiff` refers to the changes reflected in the provided
    /// description. For example, if the provided description has a server in it that this
    /// description does not, it will be returned in the `new_servers` field.
    pub(crate) fn diff(&self, other: &TopologyDescription) -> Option<TopologyDescriptionDiff> {
        if self == other {
            return None;
        }

        let addresses: HashSet<&StreamAddress> = self.server_addresses().collect();
        let other_addresses: HashSet<&StreamAddress> = other.server_addresses().collect();

        Some(TopologyDescriptionDiff {
            new_addresses: other_addresses
                .difference(&addresses)
                .cloned()
                .cloned()
                .collect(),
        })
    }

    pub(crate) fn sync_hosts(&mut self, hosts: &HashSet<StreamAddress>) {
        self.add_new_servers_from_addresses(hosts.iter());
        self.servers.retain(|host, _| hosts.contains(host));
    }

    /// Update the topology based on the new information about the topology contained by the
    /// ServerDescription.
    pub(crate) fn update(&mut self, mut server_description: ServerDescription) -> Result<()> {
        // Ignore updates from servers not currently in the cluster.
        if !self.servers.contains_key(&server_description.address) {
            return Ok(());
        }

        // Update the round trip time on the server description to the weighted average as described
        // by the spec.
        self.update_round_trip_time(&mut server_description);

        // Replace the old info about the server with the new info.
        self.servers.insert(
            server_description.address.clone(),
            server_description.clone(),
        );

        // Update the topology description based on the current topology type.
        match self.topology_type {
            TopologyType::Single => {}
            TopologyType::Unknown => self.update_unknown_topology(server_description)?,
            TopologyType::Sharded => self.update_sharded_topology(server_description),
            TopologyType::ReplicaSetNoPrimary => {
                self.update_replica_set_no_primary_topology(server_description)?
            }
            TopologyType::ReplicaSetWithPrimary => {
                self.update_replica_set_with_primary_topology(server_description)?;
            }
        }

        // Record any compatibility error.
        self.check_compatibility();

        Ok(())
    }

    /// Update the Unknown topology description based on the server description.
    fn update_unknown_topology(&mut self, server_description: ServerDescription) -> Result<()> {
        match server_description.server_type {
            ServerType::Unknown | ServerType::RSGhost => {}
            ServerType::Standalone => {
                self.update_unknown_with_standalone_server(server_description)
            }
            ServerType::Mongos => self.topology_type = TopologyType::Sharded,
            ServerType::RSPrimary => {
                self.topology_type = TopologyType::ReplicaSetWithPrimary;
                self.update_rs_from_primary_server(server_description)?;
            }
            ServerType::RSSecondary | ServerType::RSArbiter | ServerType::RSOther => {
                self.topology_type = TopologyType::ReplicaSetNoPrimary;
                self.update_rs_without_primary_server(server_description)?;
            }
        }

        Ok(())
    }

    /// Update the Sharded topology description based on the server description.
    fn update_sharded_topology(&mut self, server_description: ServerDescription) {
        match server_description.server_type {
            ServerType::Unknown | ServerType::Mongos => {}
            _ => {
                self.servers.remove(&server_description.address);
            }
        }
    }

    /// Update the ReplicaSetNoPrimary topology description based on the server description.
    fn update_replica_set_no_primary_topology(
        &mut self,
        server_description: ServerDescription,
    ) -> Result<()> {
        match server_description.server_type {
            ServerType::Unknown | ServerType::RSGhost => {}
            ServerType::Standalone | ServerType::Mongos => {
                self.servers.remove(&server_description.address);
            }
            ServerType::RSPrimary => {
                self.topology_type = TopologyType::ReplicaSetWithPrimary;
                self.update_rs_from_primary_server(server_description)?
            }
            ServerType::RSSecondary | ServerType::RSArbiter | ServerType::RSOther => {
                self.update_rs_without_primary_server(server_description)?;
            }
        }

        Ok(())
    }

    /// Update the ReplicaSetWithPrimary topology description based on the server description.
    fn update_replica_set_with_primary_topology(
        &mut self,
        server_description: ServerDescription,
    ) -> Result<()> {
        match server_description.server_type {
            ServerType::Unknown | ServerType::RSGhost => {
                self.record_primary_state();
            }
            ServerType::Standalone | ServerType::Mongos => {
                self.servers.remove(&server_description.address);
                self.record_primary_state();
            }
            ServerType::RSPrimary => self.update_rs_from_primary_server(server_description)?,
            ServerType::RSSecondary | ServerType::RSArbiter | ServerType::RSOther => {
                self.update_rs_with_primary_from_member(server_description)?;
            }
        }

        Ok(())
    }

    /// Update the Unknown topology description based on the Standalone server description.
    fn update_unknown_with_standalone_server(&mut self, server_description: ServerDescription) {
        if self.single_seed {
            self.topology_type = TopologyType::Single;
        } else {
            self.servers.remove(&server_description.address);
        }
    }

    /// Update the ReplicaSetNoPrimary topology description based on the non-primary server
    /// description.
    fn update_rs_without_primary_server(
        &mut self,
        server_description: ServerDescription,
    ) -> Result<()> {
        if self.set_name.is_none() {
            self.set_name = server_description.set_name()?;
        } else if self.set_name != server_description.set_name()? {
            self.servers.remove(&server_description.address);

            return Ok(());
        }

        self.add_new_servers(server_description.known_hosts()?)?;

        if server_description.invalid_me()? {
            self.servers.remove(&server_description.address);
        }

        Ok(())
    }

    /// Update the ReplicaSetWithPrimary topology description based on the non-primary server
    /// description.
    fn update_rs_with_primary_from_member(
        &mut self,
        server_description: ServerDescription,
    ) -> Result<()> {
        if self.set_name != server_description.set_name()? {
            self.servers.remove(&server_description.address);
            self.record_primary_state();

            return Ok(());
        }

        if server_description.invalid_me()? {
            self.servers.remove(&server_description.address);
            self.record_primary_state();

            return Ok(());
        }

        Ok(())
    }

    /// Update the replica set topology description based on the RSPrimary server description.
    fn update_rs_from_primary_server(
        &mut self,
        server_description: ServerDescription,
    ) -> Result<()> {
        if self.set_name.is_none() {
            self.set_name = server_description.set_name()?;
        } else if self.set_name != server_description.set_name()? {
            self.servers.remove(&server_description.address);
            self.record_primary_state();

            return Ok(());
        }

        if let Some(server_set_version) = server_description.set_version()? {
            if let Some(server_election_id) = server_description.election_id()? {
                if let Some(topology_max_set_version) = self.max_set_version {
                    if let Some(ref topology_max_election_id) = self.max_election_id {
                        if topology_max_set_version > server_set_version
                            || (topology_max_set_version == server_set_version
                                && *topology_max_election_id > server_election_id)
                        {
                            self.servers.insert(
                                server_description.address.clone(),
                                ServerDescription::new(server_description.address, None),
                            );
                            self.record_primary_state();
                            return Ok(());
                        }
                    }
                }

                self.max_election_id = Some(server_election_id);
            }
        }

        if let Some(server_set_version) = server_description.set_version()? {
            if self
                .max_set_version
                .as_ref()
                .map(|topology_max_set_version| server_set_version > *topology_max_set_version)
                .unwrap_or(true)
            {
                self.max_set_version = Some(server_set_version);
            }
        }

        let addresses: Vec<_> = self.servers.keys().cloned().collect();

        // If any other servers are RSPrimary, replace them with an unknown server decscription,
        // which will cause them to be updated by a new isMaster.
        for address in addresses.clone() {
            if address == server_description.address {
                continue;
            }

            if let ServerType::RSPrimary = self.servers.get(&address).unwrap().server_type {
                self.servers
                    .insert(address.clone(), ServerDescription::new(address, None));
            }
        }

        self.add_new_servers(server_description.known_hosts()?)?;
        let known_hosts: HashSet<_> = server_description.known_hosts()?.collect();

        for address in addresses {
            if !known_hosts.contains(&address.to_string()) {
                self.servers.remove(&address);
            }
        }

        self.record_primary_state();

        Ok(())
    }

    /// Inspect the topology for a primary server, and update the topology type to
    /// ReplicaSetNoPrimary if none is found.
    ///
    /// This should only be called on a replica set topology.
    fn record_primary_state(&mut self) {
        self.topology_type = if self
            .servers
            .values()
            .any(|server| server.server_type == ServerType::RSPrimary)
        {
            TopologyType::ReplicaSetWithPrimary
        } else {
            TopologyType::ReplicaSetNoPrimary
        };
    }

    /// Create a new ServerDescription for each address and add it to the topology.
    fn add_new_servers<'a>(&mut self, servers: impl Iterator<Item = &'a String>) -> Result<()> {
        let servers: Result<Vec<_>> = servers.map(|server| StreamAddress::parse(server)).collect();

        self.add_new_servers_from_addresses(servers?.iter());
        Ok(())
    }

    fn add_new_servers_from_addresses<'a>(
        &mut self,
        servers: impl Iterator<Item = &'a StreamAddress>,
    ) {
        for server in servers {
            if !self.servers.contains_key(&server) {
                self.servers
                    .insert(server.clone(), ServerDescription::new(server.clone(), None));
            }
        }
    }
}

/// A struct representing the diff between two `TopologyDescription`s.
/// Returned from `TopologyDescription::diff`.
#[derive(Debug)]
pub(crate) struct TopologyDescriptionDiff {
    pub(crate) new_addresses: HashSet<StreamAddress>,
}

fn verify_max_staleness(max_staleness: Option<Duration>) -> Result<()> {
    if max_staleness
        .map(|staleness| staleness > Duration::from_secs(0) && staleness < Duration::from_secs(90))
        .unwrap_or(false)
    {
        return Err(ErrorKind::ArgumentError {
            message: "max staleness cannot be both positive and below 90 seconds".into(),
        }
        .into());
    }

    Ok(())
}
