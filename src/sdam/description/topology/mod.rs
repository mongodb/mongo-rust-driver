pub(crate) mod server_selection;
#[cfg(test)]
mod test;

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use serde::Deserialize;

use crate::{
    bson::oid::ObjectId,
    client::ClusterTime,
    cmap::Command,
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

    /// Whether or not this topology supports sessions, and if so, what the logicalSessionTimeout
    /// is for them.
    session_support_status: SessionSupportStatus,

    /// The highest reported cluster time by any server in this topology.
    cluster_time: Option<ClusterTime>,

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
    pub(crate) fn new(options: ClientOptions) -> crate::error::Result<Self> {
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
            session_support_status: SessionSupportStatus::Undetermined,
            cluster_time: None,
            local_threshold: options.local_threshold,
            heartbeat_freq: options.heartbeat_freq,
            servers,
        })
    }

    /// Gets the topology type of the cluster.
    pub(crate) fn topology_type(&self) -> TopologyType {
        self.topology_type
    }

    pub(crate) fn server_addresses(&self) -> impl Iterator<Item = &StreamAddress> {
        self.servers.keys()
    }

    pub(crate) fn cluster_time(&self) -> Option<&ClusterTime> {
        self.cluster_time.as_ref()
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
                        options: Default::default(),
                    },
                    Some(other) => other,
                };

                command.set_read_preference(resolved_read_pref);
            }
            _ => {
                let read_pref = match criteria {
                    Some(SelectionCriteria::ReadPreference(rp)) => rp.clone(),
                    Some(SelectionCriteria::Predicate(_)) => ReadPreference::PrimaryPreferred {
                        options: Default::default(),
                    },
                    None => ReadPreference::Primary,
                };
                command.set_read_preference(read_pref);
            }
        }
    }

    fn update_command_read_pref_for_mongos(
        &self,
        command: &mut Command,
        criteria: Option<&SelectionCriteria>,
    ) {
        match criteria {
            Some(SelectionCriteria::ReadPreference(ReadPreference::Secondary { ref options })) => {
                command.set_read_preference(ReadPreference::Secondary {
                    options: options.clone(),
                });
            }
            Some(SelectionCriteria::ReadPreference(ReadPreference::PrimaryPreferred {
                ref options,
            })) => {
                command.set_read_preference(ReadPreference::PrimaryPreferred {
                    options: options.clone(),
                });
            }
            Some(SelectionCriteria::ReadPreference(ReadPreference::SecondaryPreferred {
                ref options,
            })) if options.max_staleness.is_some() || options.tag_sets.is_some() => {
                command.set_read_preference(ReadPreference::SecondaryPreferred {
                    options: options.clone(),
                });
            }
            Some(SelectionCriteria::ReadPreference(ReadPreference::Nearest { ref options })) => {
                command.set_read_preference(ReadPreference::Nearest {
                    options: options.clone(),
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

    /// Updates the topology's logical session timeout value based on the server's value for it.
    fn update_session_support_status(&mut self, server_description: &ServerDescription) {
        if !server_description.server_type.is_data_bearing() {
            return;
        }

        match server_description.logical_session_timeout().ok().flatten() {
            Some(timeout) => match self.session_support_status {
                SessionSupportStatus::Supported {
                    logical_session_timeout: topology_timeout,
                } => {
                    self.session_support_status = SessionSupportStatus::Supported {
                        logical_session_timeout: std::cmp::min(timeout, topology_timeout),
                    };
                }
                SessionSupportStatus::Undetermined => {
                    self.session_support_status = SessionSupportStatus::Supported {
                        logical_session_timeout: timeout,
                    }
                }
                SessionSupportStatus::Unsupported { .. } => {
                    // Check if the timeout is now reported on all servers, and, if so, assign the
                    // topology's timeout to the minimum.
                    let min_timeout = self
                        .servers
                        .values()
                        .filter(|s| s.server_type.is_data_bearing())
                        .map(|s| s.logical_session_timeout().ok().flatten())
                        .min()
                        .flatten();

                    match min_timeout {
                        Some(timeout) => {
                            self.session_support_status = SessionSupportStatus::Supported {
                                logical_session_timeout: timeout,
                            }
                        }
                        None => {
                            self.session_support_status = SessionSupportStatus::Unsupported {
                                logical_session_timeout: None,
                            }
                        }
                    }
                }
            },
            None if server_description.server_type.is_data_bearing()
                || self.topology_type == TopologyType::Single =>
            {
                self.session_support_status = SessionSupportStatus::Unsupported {
                    logical_session_timeout: None,
                }
            }
            None => {}
        }
    }

    /// Sets the topology's cluster time to the provided one if it is higher than the currently
    /// recorded one.
    pub(crate) fn advance_cluster_time(&mut self, cluster_time: &ClusterTime) {
        if self.cluster_time.as_ref() >= Some(cluster_time) {
            return;
        }
        self.cluster_time = Some(cluster_time.clone());
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

    /// Syncs the set of servers in the description to those in `hosts`. Servers in the set not
    /// already present in the cluster will be added, and servers in the cluster not present in the
    /// set will be removed.
    pub(crate) fn sync_hosts(&mut self, hosts: &HashSet<StreamAddress>) {
        self.add_new_servers_from_addresses(hosts.iter());
        self.servers.retain(|host, _| hosts.contains(host));
    }

    pub(crate) fn session_support_status(&self) -> SessionSupportStatus {
        self.session_support_status
    }

    /// Update the topology based on the new information about the topology contained by the
    /// ServerDescription.
    pub(crate) fn update(
        &mut self,
        mut server_description: ServerDescription,
    ) -> Result<(), String> {
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

        // Update the topology's min logicalSessionTimeout.
        self.update_session_support_status(&server_description);

        // Update the topology's max reported $clusterTime.
        if let Some(ref cluster_time) = server_description.cluster_time().ok().flatten() {
            self.advance_cluster_time(cluster_time);
        }

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
    fn update_unknown_topology(
        &mut self,
        server_description: ServerDescription,
    ) -> Result<(), String> {
        match server_description.server_type {
            ServerType::Unknown | ServerType::RsGhost => {}
            ServerType::Standalone => {
                self.update_unknown_with_standalone_server(server_description)
            }
            ServerType::Mongos => self.topology_type = TopologyType::Sharded,
            ServerType::RsPrimary => {
                self.topology_type = TopologyType::ReplicaSetWithPrimary;
                self.update_rs_from_primary_server(server_description)?;
            }
            ServerType::RsSecondary | ServerType::RsArbiter | ServerType::RsOther => {
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
    ) -> Result<(), String> {
        match server_description.server_type {
            ServerType::Unknown | ServerType::RsGhost => {}
            ServerType::Standalone | ServerType::Mongos => {
                self.servers.remove(&server_description.address);
            }
            ServerType::RsPrimary => {
                self.topology_type = TopologyType::ReplicaSetWithPrimary;
                self.update_rs_from_primary_server(server_description)?
            }
            ServerType::RsSecondary | ServerType::RsArbiter | ServerType::RsOther => {
                self.update_rs_without_primary_server(server_description)?;
            }
        }

        Ok(())
    }

    /// Update the ReplicaSetWithPrimary topology description based on the server description.
    fn update_replica_set_with_primary_topology(
        &mut self,
        server_description: ServerDescription,
    ) -> Result<(), String> {
        match server_description.server_type {
            ServerType::Unknown | ServerType::RsGhost => {
                self.record_primary_state();
            }
            ServerType::Standalone | ServerType::Mongos => {
                self.servers.remove(&server_description.address);
                self.record_primary_state();
            }
            ServerType::RsPrimary => self.update_rs_from_primary_server(server_description)?,
            ServerType::RsSecondary | ServerType::RsArbiter | ServerType::RsOther => {
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
    ) -> Result<(), String> {
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
    ) -> Result<(), String> {
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
    ) -> Result<(), String> {
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

            if let ServerType::RsPrimary = self.servers.get(&address).unwrap().server_type {
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
            .any(|server| server.server_type == ServerType::RsPrimary)
        {
            TopologyType::ReplicaSetWithPrimary
        } else {
            TopologyType::ReplicaSetNoPrimary
        };
    }

    /// Create a new ServerDescription for each address and add it to the topology.
    fn add_new_servers<'a>(
        &mut self,
        servers: impl Iterator<Item = &'a String>,
    ) -> Result<(), String> {
        let servers: Result<Vec<_>, String> = servers
            .map(|server| StreamAddress::parse(server).map_err(|e| e.to_string()))
            .collect();

        self.add_new_servers_from_addresses(servers?.iter());
        Ok(())
    }

    /// Create a new ServerDescription for each address and add it to the topology.
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

/// Enum representing whether sessions are supported by the topology.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SessionSupportStatus {
    /// It is not known yet whether the topology supports sessions. This is possible if no
    /// data-bearing servers have updated the `TopologyDescription` yet.
    Undetermined,

    /// Sessions are not supported by this topology. This is possible if there is a data-bearing
    /// server in the deployment that does not support sessions.
    ///
    /// While standalones do not support sessions, they still do report a logical session timeout,
    /// so it is stored here if necessary.
    Unsupported {
        logical_session_timeout: Option<Duration>,
    },

    /// Sessions are supported by this topology. This is the minimum timeout of all data-bearing
    /// servers in the deployment.
    Supported { logical_session_timeout: Duration },
}

impl Default for SessionSupportStatus {
    fn default() -> Self {
        Self::Undetermined
    }
}

impl SessionSupportStatus {
    #[cfg(test)]
    fn logical_session_timeout(&self) -> Option<Duration> {
        match self {
            Self::Undetermined => None,
            Self::Unsupported {
                logical_session_timeout,
            } => *logical_session_timeout,
            Self::Supported {
                logical_session_timeout,
            } => Some(*logical_session_timeout),
        }
    }
}

/// A struct representing the diff between two `TopologyDescription`s.
/// Returned from `TopologyDescription::diff`.
#[derive(Debug)]
pub(crate) struct TopologyDescriptionDiff {
    pub(crate) new_addresses: HashSet<StreamAddress>,
}

fn verify_max_staleness(max_staleness: Option<Duration>) -> crate::error::Result<()> {
    verify_max_staleness_inner(max_staleness)
        .map_err(|s| crate::error::ErrorKind::ArgumentError { message: s }.into())
}

fn verify_max_staleness_inner(max_staleness: Option<Duration>) -> Result<(), String> {
    if max_staleness
        .map(|staleness| staleness > Duration::from_secs(0) && staleness < Duration::from_secs(90))
        .unwrap_or(false)
    {
        return Err("max staleness cannot be both positive and below 90 seconds".into());
    }

    Ok(())
}
