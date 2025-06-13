#[cfg(test)]
mod test;

use std::{collections::HashMap, fmt, ops::Deref, sync::Arc, time::Duration};

use super::TopologyDescription;
use crate::{
    error::{ErrorKind, Result},
    options::ServerAddress,
    sdam::{
        description::{
            server::{ServerDescription, ServerType},
            topology::TopologyType,
        },
        Server,
        ServerInfo,
    },
    selection_criteria::{ReadPreference, SelectionCriteria, TagSet},
};

const DEFAULT_LOCAL_THRESHOLD: Duration = Duration::from_millis(15);
pub(crate) const IDLE_WRITE_PERIOD: Duration = Duration::from_secs(10);

/// Struct encapsulating a selected server that handles the opcount accounting.
#[derive(Debug)]
pub(crate) struct SelectedServer {
    server: Arc<Server>,
}

impl SelectedServer {
    fn new(server: Arc<Server>) -> Self {
        server.increment_operation_count();
        Self { server }
    }

    #[cfg(feature = "tracing-unstable")]
    pub(crate) fn address(&self) -> &ServerAddress {
        &self.server.address
    }
}

impl Deref for SelectedServer {
    type Target = Server;

    fn deref(&self) -> &Server {
        self.server.deref()
    }
}

impl Drop for SelectedServer {
    fn drop(&mut self) {
        self.server.decrement_operation_count();
    }
}

/// Attempt to select a server, returning None if no server could be selected
/// that matched the provided criteria.
pub(crate) fn attempt_to_select_server<'a>(
    criteria: &'a SelectionCriteria,
    topology_description: &'a TopologyDescription,
    servers: &'a HashMap<ServerAddress, Arc<Server>>,
    deprioritized: Option<&ServerAddress>,
) -> Result<Option<SelectedServer>> {
    let mut in_window = topology_description.suitable_servers_in_latency_window(criteria)?; // is this where we're checking for sharded clusters?
    println!("length of in_window before filter: {}", in_window.len());
    if let Some(addr) = deprioritized {
        if in_window.len() > 1 {
            in_window.retain(|d| &d.address != addr);
        }
    }
    println!("length of in_window after filter: {}", in_window.len());
    let in_window_servers = in_window
        .into_iter()
        .flat_map(|desc| servers.get(&desc.address))
        .collect();
    let selected = select_server_in_latency_window(in_window_servers);
    if let Some(server) = selected.clone() {
        println!("Selected server address: {}", server.address,);
    } else {
        eprintln!("No server was selected.");
    }
    Ok(selected.map(SelectedServer::new))
}

/// Choose a server from several suitable choices within the latency window according to
/// the algorithm laid out in the server selection specification.
fn select_server_in_latency_window(in_window: Vec<&Arc<Server>>) -> Option<Arc<Server>> {
    if in_window.is_empty() {
        return None;
    } else if in_window.len() == 1 {
        return Some(in_window[0].clone());
    }

    super::choose_n(&in_window, 2)
        .min_by_key(|s| s.operation_count())
        .map(|server| (*server).clone())
}

impl TopologyDescription {
    pub(crate) fn server_selection_timeout_error_message(
        &self,
        criteria: &SelectionCriteria,
    ) -> String {
        if self.has_available_servers() {
            format!(
                "Server selection timeout: None of the available servers suitable for criteria \
                 {:?}. Topology: {}",
                criteria, self
            )
        } else {
            format!(
                "Server selection timeout: No available servers. Topology: {}",
                self
            )
        }
    }

    pub(crate) fn suitable_servers_in_latency_window<'a>(
        &'a self,
        criteria: &'a SelectionCriteria,
    ) -> Result<Vec<&'a ServerDescription>> {
        if let Some(message) = self.compatibility_error() {
            return Err(ErrorKind::ServerSelection {
                message: message.to_string(),
            }
            .into());
        }

        let mut suitable_servers = match criteria {
            SelectionCriteria::ReadPreference(ref read_pref) => self.suitable_servers(read_pref)?,
            SelectionCriteria::Predicate(ref filter) => self
                .servers
                .values()
                .filter(|s| {
                    // If we're direct-connected or connected to a standalone, ignore whether the
                    // single server in the topology is data-bearing.
                    (self.topology_type == TopologyType::Single || s.server_type.is_data_bearing())
                        && filter(&ServerInfo::new_borrowed(s))
                })
                .collect(),
        };

        self.retain_servers_within_latency_window(&mut suitable_servers);

        Ok(suitable_servers)
    }

    pub(crate) fn has_available_servers(&self) -> bool {
        self.servers.values().any(|server| server.is_available())
    }

    fn suitable_servers(
        &self,
        read_preference: &ReadPreference,
    ) -> Result<Vec<&ServerDescription>> {
        let servers = match self.topology_type {
            TopologyType::Unknown => Vec::new(),
            TopologyType::Single | TopologyType::LoadBalanced => self.servers.values().collect(),
            TopologyType::Sharded => self.servers_with_type(&[ServerType::Mongos]).collect(),
            TopologyType::ReplicaSetWithPrimary | TopologyType::ReplicaSetNoPrimary => {
                self.suitable_servers_in_replica_set(read_preference)?
            }
        };

        Ok(servers)
    }

    fn retain_servers_within_latency_window(&self, suitable_servers: &mut Vec<&ServerDescription>) {
        let shortest_average_rtt = suitable_servers
            .iter()
            .filter_map(|server_desc| server_desc.average_round_trip_time)
            .fold(Option::<Duration>::None, |min, curr| match min {
                Some(prev) => Some(prev.min(curr)),
                None => Some(curr),
            });

        let local_threshold = self.local_threshold.unwrap_or(DEFAULT_LOCAL_THRESHOLD);

        let max_rtt_within_window = shortest_average_rtt
            .map(|rtt| rtt.checked_add(local_threshold).unwrap_or(Duration::MAX));

        suitable_servers.retain(move |server_desc| {
            if let Some(server_rtt) = server_desc.average_round_trip_time {
                // unwrap() is safe here because this server's avg rtt being Some indicates that
                // there exists a max rtt as well.
                server_rtt <= max_rtt_within_window.unwrap()
            } else {
                // SDAM isn't performed with a load balanced topology, so the load balancer won't
                // have an RTT. Instead, we just select it.
                matches!(server_desc.server_type, ServerType::LoadBalancer)
            }
        });
    }

    pub(crate) fn servers_with_type<'a>(
        &'a self,
        types: &'a [ServerType],
    ) -> impl Iterator<Item = &'a ServerDescription> {
        self.servers
            .values()
            .filter(move |server| types.contains(&server.server_type))
    }

    #[cfg(any(test, feature = "in-use-encryption"))]
    pub(crate) fn primary(&self) -> Option<&ServerDescription> {
        self.servers_with_type(&[ServerType::RsPrimary]).next()
    }

    fn suitable_servers_in_replica_set(
        &self,
        read_preference: &ReadPreference,
    ) -> Result<Vec<&ServerDescription>> {
        let tag_sets = read_preference.tag_sets();
        let max_staleness = read_preference.max_staleness();

        let servers = match read_preference {
            ReadPreference::Primary => self.servers_with_type(&[ServerType::RsPrimary]).collect(),
            ReadPreference::Secondary { .. } => self.suitable_servers_for_read_preference(
                &[ServerType::RsSecondary],
                tag_sets,
                max_staleness,
            )?,
            ReadPreference::PrimaryPreferred { .. } => {
                match self.servers_with_type(&[ServerType::RsPrimary]).next() {
                    Some(primary) => vec![primary],
                    None => self.suitable_servers_for_read_preference(
                        &[ServerType::RsSecondary],
                        tag_sets,
                        max_staleness,
                    )?,
                }
            }
            ReadPreference::SecondaryPreferred { .. } => {
                let suitable_servers = self.suitable_servers_for_read_preference(
                    &[ServerType::RsSecondary],
                    tag_sets,
                    max_staleness,
                )?;

                if suitable_servers.is_empty() {
                    self.servers_with_type(&[ServerType::RsPrimary]).collect()
                } else {
                    suitable_servers
                }
            }
            ReadPreference::Nearest { .. } => self.suitable_servers_for_read_preference(
                &[ServerType::RsPrimary, ServerType::RsSecondary],
                tag_sets,
                max_staleness,
            )?,
        };

        Ok(servers)
    }

    fn suitable_servers_for_read_preference(
        &self,
        types: &'static [ServerType],
        tag_sets: Option<&Vec<TagSet>>,
        max_staleness: Option<Duration>,
    ) -> Result<Vec<&ServerDescription>> {
        if let Some(max_staleness) = max_staleness {
            super::verify_max_staleness(max_staleness, self.heartbeat_frequency())?;
        }

        let mut servers = self.servers_with_type(types).collect();

        // We don't need to check for the Client's default max_staleness because it would be passed
        // in as part of the Client's default ReadPreference if none is specified for the operation.
        if let Some(max_staleness) = max_staleness {
            // According to the spec, max staleness <= 0 is the same as no max staleness.
            if max_staleness > Duration::from_secs(0) {
                self.filter_servers_by_max_staleness(&mut servers, max_staleness);
            }
        }

        if let Some(tag_sets) = tag_sets {
            filter_servers_by_tag_sets(&mut servers, tag_sets);
        }

        Ok(servers)
    }

    fn filter_servers_by_max_staleness(
        &self,
        servers: &mut Vec<&ServerDescription>,
        max_staleness: Duration,
    ) {
        let primary = self
            .servers
            .values()
            .find(|server| server.server_type == ServerType::RsPrimary);

        match primary {
            Some(primary) => {
                self.filter_servers_by_max_staleness_with_primary(servers, primary, max_staleness)
            }
            None => self.filter_servers_by_max_staleness_without_primary(servers, max_staleness),
        };
    }

    fn filter_servers_by_max_staleness_with_primary(
        &self,
        servers: &mut Vec<&ServerDescription>,
        primary: &ServerDescription,
        max_staleness: Duration,
    ) {
        let max_staleness_ms = max_staleness.as_millis().try_into().unwrap_or(i64::MAX);

        servers.retain(|server| {
            let server_staleness = self.calculate_secondary_staleness_with_primary(server, primary);

            server_staleness
                .map(|staleness| staleness <= max_staleness_ms)
                .unwrap_or(false)
        })
    }

    fn filter_servers_by_max_staleness_without_primary(
        &self,
        servers: &mut Vec<&ServerDescription>,
        max_staleness: Duration,
    ) {
        let max_staleness = max_staleness.as_millis().try_into().unwrap_or(i64::MAX);
        let max_write_date = self
            .servers
            .values()
            .filter(|server| server.server_type == ServerType::RsSecondary)
            .filter_map(|server| {
                server
                    .last_write_date()
                    .ok()
                    .and_then(std::convert::identity)
            })
            .map(|last_write_date| last_write_date.timestamp_millis())
            .max();

        let secondary_max_write_date = match max_write_date {
            Some(max_write_date) => max_write_date,
            None => return,
        };

        servers.retain(|server| {
            let server_staleness = self
                .calculate_secondary_staleness_without_primary(server, secondary_max_write_date);

            server_staleness
                .map(|staleness| staleness <= max_staleness)
                .unwrap_or(false)
        })
    }

    fn calculate_secondary_staleness_with_primary(
        &self,
        secondary: &ServerDescription,
        primary: &ServerDescription,
    ) -> Option<i64> {
        let primary_last_update = primary.last_update_time?.timestamp_millis();
        let primary_last_write = primary.last_write_date().ok()??.timestamp_millis();

        let secondary_last_update = secondary.last_update_time?.timestamp_millis();
        let secondary_last_write = secondary.last_write_date().ok()??.timestamp_millis();

        let heartbeat_frequency = self
            .heartbeat_frequency()
            .as_millis()
            .try_into()
            .unwrap_or(i64::MAX);

        let staleness = (secondary_last_update - secondary_last_write)
            - (primary_last_update - primary_last_write)
            + heartbeat_frequency;

        Some(staleness)
    }

    fn calculate_secondary_staleness_without_primary(
        &self,
        secondary: &ServerDescription,
        max_last_write_date: i64,
    ) -> Option<i64> {
        let secondary_last_write = secondary.last_write_date().ok()??.timestamp_millis();
        let heartbeat_frequency = self
            .heartbeat_frequency()
            .as_millis()
            .try_into()
            .unwrap_or(i64::MAX);

        let staleness = max_last_write_date - secondary_last_write + heartbeat_frequency;
        Some(staleness)
    }
}

impl fmt::Display for TopologyDescription {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
        write!(f, "{{ Type: {}", self.topology_type)?;

        if let Some(ref set_name) = self.set_name {
            write!(f, ", Set Name: {}", set_name)?;
        }

        if let Some(max_set_version) = self.max_set_version {
            write!(f, ", Max Set Version: {}", max_set_version)?;
        }

        if let Some(max_election_id) = self.max_election_id {
            write!(f, ", Max Election ID: {}", max_election_id)?;
        }

        if let Some(ref compatibility_error) = self.compatibility_error {
            write!(f, ", Compatibility Error: {}", compatibility_error)?;
        }

        if !self.servers.is_empty() {
            write!(f, ", Servers: [ ")?;
            let mut iter = self.servers.values();
            if let Some(server) = iter.next() {
                write!(f, "{}", ServerInfo::new_borrowed(server))?;
            }
            for server in iter {
                write!(f, ", {}", ServerInfo::new_borrowed(server))?;
            }
            write!(f, " ]")?;
        }

        write!(f, " }}")
    }
}

fn filter_servers_by_tag_sets(servers: &mut Vec<&ServerDescription>, tag_sets: &[TagSet]) {
    if tag_sets.is_empty() {
        return;
    }

    for tag_set in tag_sets {
        let matches_tag_set = |server: &&ServerDescription| server.matches_tag_set(tag_set);

        if servers.iter().any(matches_tag_set) {
            servers.retain(matches_tag_set);

            return;
        }
    }

    servers.clear();
}
