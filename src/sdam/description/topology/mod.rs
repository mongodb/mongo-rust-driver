pub(crate) mod server_selection;
#[cfg(test)]
pub(crate) mod test;

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::{
    bson::oid::ObjectId,
    client::ClusterTime,
    cmap::Command,
    error::{Error, Result},
    options::{ClientOptions, ServerAddress},
    sdam::{
        description::server::{ServerDescription, ServerType},
        DEFAULT_HEARTBEAT_FREQUENCY,
    },
    selection_criteria::{ReadPreference, SelectionCriteria},
};

use self::server_selection::IDLE_WRITE_PERIOD;

/// The possible types for a topology.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize)]
#[non_exhaustive]
pub enum TopologyType {
    /// A single mongod server.
    Single,

    /// A replica set with no primary.
    ReplicaSetNoPrimary,

    /// A replica set with a primary.
    ReplicaSetWithPrimary,

    /// A sharded topology.
    Sharded,

    /// A load balanced topology.
    LoadBalanced,

    /// A topology whose type is not known.
    Unknown,
}

#[cfg(test)]
impl TopologyType {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Single => "Single",
            Self::ReplicaSetNoPrimary => "ReplicaSetNoPrimary",
            Self::ReplicaSetWithPrimary => "ReplicaSetWithPrimary",
            Self::Sharded => "Sharded",
            Self::LoadBalanced => "LoadBalanced",
            Self::Unknown => "Unknown",
        }
    }
}

impl Default for TopologyType {
    fn default() -> Self {
        TopologyType::Unknown
    }
}

/// A description of the most up-to-date information known about a topology.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub(crate) struct TopologyDescription {
    /// Whether or not the topology was initialized with a single seed.
    #[serde(skip)]
    pub(crate) single_seed: bool,

    /// The current type of the topology.
    pub(crate) topology_type: TopologyType,

    /// The replica set name of the topology.
    pub(crate) set_name: Option<String>,

    /// The highest replica set version the driver has seen by a member of the topology.
    pub(crate) max_set_version: Option<i32>,

    /// The highest replica set election id the driver has seen by a member of the topology.
    pub(crate) max_election_id: Option<ObjectId>,

    /// Describes the compatibility issue between the driver and server with regards to the
    /// respective supported wire versions.
    pub(crate) compatibility_error: Option<String>,

    /// Whether or not this topology supports sessions, and if so, what the logicalSessionTimeout
    /// is for them.
    #[serde(skip)]
    pub(crate) session_support_status: SessionSupportStatus,

    /// Whether or not this topology supports transactions.
    #[serde(skip)]
    pub(crate) transaction_support_status: TransactionSupportStatus,

    /// The highest reported cluster time by any server in this topology.
    #[serde(skip)]
    pub(crate) cluster_time: Option<ClusterTime>,

    /// The amount of latency beyond that of the suitable server with the minimum latency that is
    /// acceptable for a read operation.
    #[serde(skip)]
    pub(crate) local_threshold: Option<Duration>,

    /// The maximum amount of time to wait before checking a given server by sending server check.
    #[serde(skip)]
    pub(crate) heartbeat_freq: Option<Duration>,

    /// The server descriptions of each member of the topology.
    pub(crate) servers: HashMap<ServerAddress, ServerDescription>,
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

impl Default for TopologyDescription {
    fn default() -> Self {
        Self {
            single_seed: false,
            topology_type: TopologyType::Unknown,
            set_name: Default::default(),
            max_set_version: Default::default(),
            max_election_id: Default::default(),
            compatibility_error: Default::default(),
            session_support_status: SessionSupportStatus::Undetermined,
            transaction_support_status: TransactionSupportStatus::Undetermined,
            cluster_time: Default::default(),
            local_threshold: Default::default(),
            heartbeat_freq: Default::default(),
            servers: Default::default(),
        }
    }
}

impl TopologyDescription {
    pub(crate) fn initialize(&mut self, options: &ClientOptions) {
        debug_assert!(
            self.servers.is_empty() && self.topology_type == TopologyType::Unknown,
            "new TopologyDescriptions should start empty"
        );

        self.topology_type = if let Some(true) = options.direct_connection {
            TopologyType::Single
        } else if options.repl_set_name.is_some() {
            TopologyType::ReplicaSetNoPrimary
        } else if options.load_balanced.unwrap_or(false) {
            TopologyType::LoadBalanced
        } else {
            TopologyType::Unknown
        };

        self.session_support_status = if self.topology_type == TopologyType::LoadBalanced {
            SessionSupportStatus::Supported {
                logical_session_timeout: None,
            }
        } else {
            SessionSupportStatus::Undetermined
        };

        self.transaction_support_status = if self.topology_type == TopologyType::LoadBalanced {
            TransactionSupportStatus::Supported
        } else {
            TransactionSupportStatus::Undetermined
        };

        for address in options.hosts.iter() {
            let description = ServerDescription::new(address.clone());
            self.servers.insert(address.to_owned(), description);
        }

        self.single_seed = self.servers.len() == 1;
        self.set_name = options.repl_set_name.clone();
        self.local_threshold = options.local_threshold;
        self.heartbeat_freq = options.heartbeat_freq;
    }

    /// Gets the topology type of the cluster.
    pub(crate) fn topology_type(&self) -> TopologyType {
        self.topology_type
    }

    pub(crate) fn server_addresses(&self) -> impl Iterator<Item = &ServerAddress> {
        self.servers.keys()
    }

    pub(crate) fn cluster_time(&self) -> Option<&ClusterTime> {
        self.cluster_time.as_ref()
    }

    pub(crate) fn get_server_description(
        &self,
        address: &ServerAddress,
    ) -> Option<&ServerDescription> {
        self.servers.get(address)
    }

    pub(crate) fn update_command_with_read_pref<T>(
        &self,
        address: &ServerAddress,
        command: &mut Command<T>,
        criteria: Option<&SelectionCriteria>,
    ) {
        let server_type = self
            .get_server_description(address)
            .map(|sd| sd.server_type)
            .unwrap_or(ServerType::Unknown);

        match (self.topology_type, server_type) {
            (TopologyType::Sharded, ServerType::Mongos)
            | (TopologyType::Single, ServerType::Mongos)
            | (TopologyType::LoadBalanced, _) => {
                self.update_command_read_pref_for_mongos(command, criteria)
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

                command.set_read_preference(resolved_read_pref)
            }
            _ => {
                let read_pref = match criteria {
                    Some(SelectionCriteria::ReadPreference(rp)) => rp.clone(),
                    Some(SelectionCriteria::Predicate(_)) => ReadPreference::PrimaryPreferred {
                        options: Default::default(),
                    },
                    None => ReadPreference::Primary,
                };
                command.set_read_preference(read_pref)
            }
        }
    }

    fn update_command_read_pref_for_mongos<T>(
        &self,
        command: &mut Command<T>,
        criteria: Option<&SelectionCriteria>,
    ) {
        let read_preference = match criteria {
            Some(SelectionCriteria::ReadPreference(rp)) => rp,
            _ => return,
        };
        match read_preference {
            ReadPreference::Secondary { .. }
            | ReadPreference::PrimaryPreferred { .. }
            | ReadPreference::Nearest { .. }
            | ReadPreference::SecondaryPreferred { .. } => {
                command.set_read_preference(read_preference.clone())
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

    /// Updates the topology's logical session timeout value based on the server's value for it.
    fn update_session_support_status(&mut self, server_description: &ServerDescription) {
        if !server_description.server_type.is_data_bearing() {
            if let TopologyType::Single = self.topology_type {
                self.session_support_status = SessionSupportStatus::Unsupported {
                    logical_session_timeout: None,
                };
            }
            return;
        }

        match server_description.logical_session_timeout().ok().flatten() {
            Some(timeout) => match self.session_support_status {
                SessionSupportStatus::Supported {
                    logical_session_timeout: topology_timeout,
                } => {
                    self.session_support_status = SessionSupportStatus::Supported {
                        logical_session_timeout: std::cmp::min(Some(timeout), topology_timeout),
                    };
                }
                SessionSupportStatus::Undetermined => {
                    self.session_support_status = SessionSupportStatus::Supported {
                        logical_session_timeout: Some(timeout),
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
                                logical_session_timeout: Some(timeout),
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

    /// Updates the topology's transaction support status based on its session support status and
    /// the server description's max wire version.
    fn update_transaction_support_status(&mut self, server_description: &ServerDescription) {
        if !matches!(
            self.session_support_status,
            SessionSupportStatus::Supported { .. }
        ) {
            self.transaction_support_status = TransactionSupportStatus::Unsupported;
        }
        if let Ok(Some(max_wire_version)) = server_description.max_wire_version() {
            self.transaction_support_status = if max_wire_version < 7
                || (max_wire_version < 8 && self.topology_type == TopologyType::Sharded)
            {
                TransactionSupportStatus::Unsupported
            } else {
                TransactionSupportStatus::Supported
            }
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
    /// description does not, it will be returned in the `added_addresses` field.
    pub(crate) fn diff<'a>(
        &'a self,
        other: &'a TopologyDescription,
    ) -> Option<TopologyDescriptionDiff> {
        if self == other {
            return None;
        }

        let addresses: HashSet<&ServerAddress> = self.server_addresses().collect();
        let other_addresses: HashSet<&ServerAddress> = other.server_addresses().collect();

        let changed_servers = self
            .servers
            .iter()
            .filter_map(|(address, description)| match other.servers.get(address) {
                Some(other_description) if description != other_description => {
                    Some((address, (description, other_description)))
                }
                _ => None,
            });

        Some(TopologyDescriptionDiff {
            removed_addresses: addresses.difference(&other_addresses).cloned().collect(),
            added_addresses: other_addresses.difference(&addresses).cloned().collect(),
            changed_servers: changed_servers.collect(),
        })
    }

    /// Syncs the set of servers in the description to those in `hosts`. Servers in the set not
    /// already present in the cluster will be added, and servers in the cluster not present in the
    /// set will be removed.
    pub(crate) fn sync_hosts(&mut self, hosts: &HashSet<ServerAddress>) {
        self.add_new_servers_from_addresses(hosts.iter());
        self.servers.retain(|host, _| hosts.contains(host));
    }

    pub(crate) fn session_support_status(&self) -> SessionSupportStatus {
        self.session_support_status
    }

    pub(crate) fn transaction_support_status(&self) -> TransactionSupportStatus {
        self.transaction_support_status
    }

    /// Update the topology based on the new information about the topology contained by the
    /// ServerDescription.
    pub(crate) fn update(&mut self, mut server_description: ServerDescription) -> Result<()> {
        match self.servers.get(&server_description.address) {
            None => return Ok(()),
            Some(existing_sd) => {
                // Ignore updates from outdated topology versions.
                if let Some(existing_tv) = existing_sd.topology_version() {
                    if let Some(new_tv) = server_description.topology_version() {
                        if existing_tv.process_id == new_tv.process_id
                            && new_tv.counter < existing_tv.counter
                        {
                            return Ok(());
                        }
                    }
                }
            }
        }

        if let Some(expected_name) = &self.set_name {
            if server_description.is_available() {
                let got_name = server_description.set_name();
                if self.topology_type() == TopologyType::Single
                    && !matches!(
                        got_name.as_ref().map(|opt| opt.as_ref()),
                        Ok(Some(name)) if name == expected_name
                    )
                {
                    let got_display = match got_name {
                        Ok(Some(s)) => format!("{:?}", s),
                        Ok(None) => "<none>".to_string(),
                        Err(s) => format!("<error: {}>", s),
                    };
                    // Mark server as unknown.
                    server_description = ServerDescription::new_from_error(
                        server_description.address,
                        Error::invalid_argument(format!(
                            "Connection string replicaSet name {:?} does not match actual name {}",
                            expected_name, got_display,
                        )),
                    );
                }
            }
        }

        // Replace the old info about the server with the new info.
        self.servers.insert(
            server_description.address.clone(),
            server_description.clone(),
        );

        if let TopologyType::LoadBalanced = self.topology_type {
            // Load-balanced topologies don't have real server updates; attempting to update based
            // on the synthesized one causes incorrect behavior.
            return Ok(());
        }

        // Update the topology's min logicalSessionTimeout.
        self.update_session_support_status(&server_description);

        // Update the topology's transaction support status.
        self.update_transaction_support_status(&server_description);

        // Update the topology's max reported $clusterTime.
        if let Some(ref cluster_time) = server_description.cluster_time().ok().flatten() {
            self.advance_cluster_time(cluster_time);
        }

        // Update the topology description based on the current topology type.
        match self.topology_type {
            TopologyType::Single | TopologyType::LoadBalanced => {}
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
            ServerType::LoadBalancer => {
                return Err(Error::internal("cannot transition to a load balancer"))
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
            ServerType::LoadBalancer => {
                return Err(Error::internal("cannot transition to a load balancer"))
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
            ServerType::LoadBalancer => {
                return Err(Error::internal("cannot transition to a load balancer"));
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
                                ServerDescription::new(server_description.address),
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
        // which will cause them to be updated by a new server check.
        for address in addresses.clone() {
            if address == server_description.address {
                continue;
            }

            if let ServerType::RsPrimary = self.servers.get(&address).unwrap().server_type {
                self.servers
                    .insert(address.clone(), ServerDescription::new(address));
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
    fn add_new_servers<'a>(&mut self, servers: impl Iterator<Item = &'a String>) -> Result<()> {
        let servers: Result<Vec<_>> = servers.map(ServerAddress::parse).collect();

        self.add_new_servers_from_addresses(servers?.iter());
        Ok(())
    }

    /// Create a new ServerDescription for each address and add it to the topology.
    fn add_new_servers_from_addresses<'a>(
        &mut self,
        servers: impl Iterator<Item = &'a ServerAddress>,
    ) {
        for server in servers {
            if !self.servers.contains_key(server) {
                self.servers
                    .insert(server.clone(), ServerDescription::new(server.clone()));
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
    Supported {
        logical_session_timeout: Option<Duration>,
    },
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
            } => *logical_session_timeout,
        }
    }
}

/// Enum representing whether transactions are supported by the topology.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum TransactionSupportStatus {
    /// It is not known yet whether the topology supports transactions. This is possible if no
    /// data-bearing servers have updated the `TopologyDescription` yet.
    Undetermined,

    /// Transactions are not supported by this topology.
    Unsupported,

    /// Transactions are supported by this topology. A topology supports transactions if it
    /// supports sessions and its maxWireVersion >= 7. If the topology is sharded, maxWireVersion
    /// must be >= 8 for transactions to be supported.
    ///
    /// Note that meeting these conditions does not guarantee that a deployment
    /// supports transactions; any other missing qualification will be reported by the server.
    Supported,
}

impl Default for TransactionSupportStatus {
    fn default() -> Self {
        Self::Undetermined
    }
}

/// A struct representing the diff between two `TopologyDescription`s.
/// Returned from `TopologyDescription::diff`.
#[derive(Debug)]
pub(crate) struct TopologyDescriptionDiff<'a> {
    pub(crate) removed_addresses: HashSet<&'a ServerAddress>,
    pub(crate) added_addresses: HashSet<&'a ServerAddress>,
    pub(crate) changed_servers:
        HashMap<&'a ServerAddress, (&'a ServerDescription, &'a ServerDescription)>,
}

pub(crate) fn verify_max_staleness(
    max_staleness: Duration,
    heartbeat_frequency: Duration,
) -> crate::error::Result<()> {
    let smallest_max_staleness = std::cmp::max(
        Duration::from_secs(90),
        heartbeat_frequency
            .checked_add(IDLE_WRITE_PERIOD)
            .unwrap_or(Duration::MAX),
    );

    if max_staleness < smallest_max_staleness {
        return Err(Error::invalid_argument(format!(
            "invalid max_staleness value: must be at least {} seconds",
            smallest_max_staleness.as_secs()
        )));
    }

    Ok(())
}
