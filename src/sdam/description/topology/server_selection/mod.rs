#[cfg(test)]
mod test;

use std::{collections::HashMap, fmt, ops::Deref, sync::Arc, time::Duration};

use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};

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
) -> Result<Option<SelectedServer>> {
    let in_window = topology_description.suitable_servers_in_latency_window(criteria)?;
    let in_window_servers = in_window
        .into_iter()
        .flat_map(|desc| servers.get(&desc.address))
        .collect();
    Ok(select_server_in_latency_window(in_window_servers).map(SelectedServer::new))
}

/// Choose a server from several suitable choices within the latency window according to
/// the algorithm laid out in the server selection specification.
fn select_server_in_latency_window(in_window: Vec<&Arc<Server>>) -> Option<Arc<Server>> {
    if in_window.is_empty() {
        return None;
    } else if in_window.len() == 1 {
        return Some(in_window[0].clone());
    }

    let mut rng = SmallRng::from_entropy();
    in_window
        .choose_multiple(&mut rng, 2)
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
                .filter(|s| s.server_type.is_data_bearing() && filter(&ServerInfo::new_borrowed(s)))
                .collect(),
        };

        self.retain_servers_within_latency_window(&mut suitable_servers);

        Ok(suitable_servers)
    }

    pub(crate) fn has_available_servers(&self) -> bool {
        self.servers.values().any(|server| server.is_available())
    }

    fn suitable_servers<'a>(
        &self,
        read_preference: &'a ReadPreference,
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

    fn retain_servers_within_latency_window<'a>(
        &self,
        suitable_servers: &mut Vec<&'a ServerDescription>,
    ) {
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

    #[cfg(test)]
    pub(crate) fn primary(&self) -> Option<&ServerDescription> {
        self.servers_with_type(&[ServerType::RsPrimary]).next()
    }

    fn suitable_servers_in_replica_set<'a>(
        &self,
        read_preference: &'a ReadPreference,
    ) -> Result<Vec<&ServerDescription>> {
        let servers = match read_preference {
            ReadPreference::Primary => self.servers_with_type(&[ServerType::RsPrimary]).collect(),
            ReadPreference::Secondary { ref options } => self
                .suitable_servers_for_read_preference(
                    &[ServerType::RsSecondary],
                    options.tag_sets.as_ref(),
                    options.max_staleness,
                )?,
            ReadPreference::PrimaryPreferred { ref options } => {
                match self.servers_with_type(&[ServerType::RsPrimary]).next() {
                    Some(primary) => vec![primary],
                    None => self.suitable_servers_for_read_preference(
                        &[ServerType::RsSecondary],
                        options.tag_sets.as_ref(),
                        options.max_staleness,
                    )?,
                }
            }
            ReadPreference::SecondaryPreferred { ref options } => {
                let suitable_servers = self.suitable_servers_for_read_preference(
                    &[ServerType::RsSecondary],
                    options.tag_sets.as_ref(),
                    options.max_staleness,
                )?;

                if suitable_servers.is_empty() {
                    self.servers_with_type(&[ServerType::RsPrimary]).collect()
                } else {
                    suitable_servers
                }
            }
            ReadPreference::Nearest { ref options } => self.suitable_servers_for_read_preference(
                &[ServerType::RsPrimary, ServerType::RsSecondary],
                options.tag_sets.as_ref(),
                options.max_staleness,
            )?,
        };

        Ok(servers)
    }

    fn suitable_servers_for_read_preference<'a>(
        &self,
        types: &'static [ServerType],
        tag_sets: Option<&'a Vec<TagSet>>,
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
        let max_staleness_ms = max_staleness.as_millis() as i64;

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
        let max_staleness = max_staleness.as_millis() as i64;
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

        let heartbeat_frequency = self.heartbeat_frequency().as_millis() as i64;

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
        let heartbeat_frequency = self.heartbeat_frequency().as_millis() as i64;

        let staleness = max_last_write_date - secondary_last_write + heartbeat_frequency;
        Some(staleness)
    }
}

impl fmt::Display for TopologyDescription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{ Type: {:?}, Servers: [ ", self.topology_type)?;
        for server_info in self.servers.values().map(ServerInfo::new_borrowed) {
            write!(f, "{}, ", server_info)?;
        }
        write!(f, "] }}")
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
