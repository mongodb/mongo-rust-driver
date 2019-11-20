#[cfg(test)]
mod test;

use std::time::Duration;

use rand::seq::IteratorRandom;

use super::TopologyDescription;
use crate::{
    error::Result,
    sdam::{
        description::{
            server::{ServerDescription, ServerType},
            topology::TopologyType,
        },
        public::ServerInfo,
    },
    selection_criteria::{ReadPreference, SelectionCriteria, TagSet},
};

const DEFAULT_LOCAL_THRESHOLD: Duration = Duration::from_millis(15);

impl TopologyDescription {
    pub(crate) fn select_server<'a>(
        &'a self,
        criteria: &'a SelectionCriteria,
    ) -> Result<Option<&'a ServerDescription>> {
        // Although `suitable_servers` has to handle the Unknown case for testing, we skip calling
        // it here to avoid an allocation.
        if let TopologyType::Unknown = self.topology_type {
            return Ok(None);
        }

        // Similarly, if the topology type is Single, we skip the below logic as well and just
        // return the only server in the topology.
        if let TopologyType::Single = self.topology_type {
            return Ok(self.servers.values().next());
        }

        let mut suitable_servers = match criteria {
            SelectionCriteria::ReadPreference(ref read_pref) => self.suitable_servers(read_pref)?,
            SelectionCriteria::Predicate(ref filter) => self
                .servers
                .values()
                .filter(|s| filter(&ServerInfo::new(s)))
                .collect(),
        };

        // If the read preference is primary, we skip the overhead of calculating the latency window
        // because we know that there's only one server selected.
        if !criteria.is_read_pref_primary() {
            self.retain_servers_within_latency_window(&mut suitable_servers);
        }

        // Choose a random server from the remaining set.
        Ok(suitable_servers.into_iter().choose(&mut rand::thread_rng()))
    }

    fn suitable_servers<'a>(
        &'a self,
        read_preference: &'a ReadPreference,
    ) -> Result<Vec<&'a ServerDescription>> {
        let servers = match self.topology_type {
            TopologyType::Unknown => Vec::new(),
            TopologyType::Single => self.servers.values().collect(),
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

        let max_rtt_within_window = shortest_average_rtt.map(|rtt| rtt + local_threshold);

        suitable_servers.retain(move |server_desc| {
            if let Some(server_rtt) = server_desc.average_round_trip_time {
                if let Some(max_rtt) = max_rtt_within_window {
                    return server_rtt <= max_rtt;
                }
            }

            false
        });
    }

    fn servers_with_type<'a>(
        &'a self,
        types: &'a [ServerType],
    ) -> impl Iterator<Item = &'a ServerDescription> {
        self.servers
            .values()
            .filter(move |server| types.contains(&server.server_type))
    }

    fn suitable_servers_in_replica_set<'a>(
        &'a self,
        read_preference: &'a ReadPreference,
    ) -> Result<Vec<&'a ServerDescription>> {
        let servers = match read_preference {
            ReadPreference::Primary => self.servers_with_type(&[ServerType::RSPrimary]).collect(),
            ReadPreference::Secondary {
                ref tag_sets,
                max_staleness,
            } => self.suitable_servers_for_read_preference(
                &[ServerType::RSSecondary],
                tag_sets.as_ref(),
                *max_staleness,
            )?,
            ReadPreference::PrimaryPreferred {
                ref tag_sets,
                max_staleness,
            } => match self.servers_with_type(&[ServerType::RSPrimary]).next() {
                Some(primary) => vec![primary],
                None => self.suitable_servers_for_read_preference(
                    &[ServerType::RSSecondary],
                    tag_sets.as_ref(),
                    *max_staleness,
                )?,
            },
            ReadPreference::SecondaryPreferred {
                ref tag_sets,
                max_staleness,
            } => {
                let suitable_servers = self.suitable_servers_for_read_preference(
                    &[ServerType::RSSecondary],
                    tag_sets.as_ref(),
                    *max_staleness,
                )?;

                if suitable_servers.is_empty() {
                    self.servers_with_type(&[ServerType::RSPrimary]).collect()
                } else {
                    suitable_servers
                }
            }
            ReadPreference::Nearest {
                ref tag_sets,
                max_staleness,
            } => self.suitable_servers_for_read_preference(
                &[ServerType::RSPrimary, ServerType::RSSecondary],
                tag_sets.as_ref(),
                *max_staleness,
            )?,
        };

        Ok(servers)
    }

    fn suitable_servers_for_read_preference<'a>(
        &'a self,
        types: &'a [ServerType],
        tag_sets: Option<&'a Vec<TagSet>>,
        max_staleness: Option<Duration>,
    ) -> Result<Vec<&'a ServerDescription>> {
        super::verify_max_staleness(max_staleness)?;

        let mut servers = self.servers_with_type(types).collect();

        if let Some(max_staleness) = max_staleness.or(self.max_staleness) {
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
            .find(|server| server.server_type == ServerType::RSPrimary);

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
            .filter(|server| server.server_type == ServerType::RSSecondary)
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
