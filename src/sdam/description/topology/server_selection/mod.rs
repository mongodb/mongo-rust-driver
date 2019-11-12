#[cfg(test)]
mod test;

use std::time::Duration;

use derivative::Derivative;
use rand::seq::IteratorRandom;

use super::{TopologyDescription, TopologyType};
use crate::{
    read_preference::{ReadPreference, TagSet},
    sdam::description::server::{ServerDescription, ServerType},
};

/// Describes which servers are suitable for a given operation.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) enum SelectionCriteria {
    /// A read preference that describes the suitable servers based on the server type, max
    /// staleness, and server tags.
    ReadPreference(ReadPreference),

    /// A predicate used to filter servers that are considered suitable. A `server` will be
    /// considered suitable by a `predicate` if `predicate(server)` returns true.
    Predicate(#[derivative(Debug = "ignore")] Box<dyn Fn(&ServerDescription) -> bool>),
}

impl SelectionCriteria {
    pub(crate) fn as_read_pref(&self) -> Option<&ReadPreference> {
        match self {
            Self::ReadPreference(ref read_pref) => Some(read_pref),
            Self::Predicate(..) => None,
        }
    }
}

impl SelectionCriteria {
    fn is_read_pref_primary(&self) -> bool {
        match self {
            Self::ReadPreference(ReadPreference::Primary) => true,
            _ => false,
        }
    }
}

const DEFAULT_LOCAL_THRESHOLD: Duration = Duration::from_millis(15);

impl TopologyDescription {
    pub(crate) fn select_server<'a>(
        &'a self,
        criteria: &'a SelectionCriteria,
    ) -> Option<&'a ServerDescription> {
        // Although `suitable_servers` has to handle the Unknown case for testing, we skip calling
        // it here to avoid an allocation.
        if let TopologyType::Unknown = self.topology_type {
            return None;
        }

        // Similarly, if the topology type is Single, we skip the below logic as well and just
        // return the only server in the topology.
        if let TopologyType::Single = self.topology_type {
            return self.servers.values().next();
        }

        let mut suitable_servers = match criteria {
            SelectionCriteria::ReadPreference(ref read_pref) => self.suitable_servers(read_pref),
            SelectionCriteria::Predicate(ref filter) => {
                self.servers.values().filter(|s| filter(s)).collect()
            }
        };

        // If the read preference is primary, we skip the overhead of calculating the latency window
        // because we know that there's only one server selected.
        if !criteria.is_read_pref_primary() {
            self.retain_servers_within_latency_window(&mut suitable_servers);
        }

        // Choose a random server from the remaining set.
        suitable_servers.into_iter().choose(&mut rand::thread_rng())
    }

    fn suitable_servers<'a>(
        &'a self,
        read_preference: &'a ReadPreference,
    ) -> Vec<&'a ServerDescription> {
        match self.topology_type {
            TopologyType::Unknown => Vec::new(),
            TopologyType::Single => self.servers.values().collect(),
            TopologyType::Sharded => self.servers_with_type(&[ServerType::Mongos]).collect(),
            TopologyType::ReplicaSetWithPrimary | TopologyType::ReplicaSetNoPrimary => {
                self.suitable_servers_in_replica_set(read_preference)
            }
        }
    }

    fn retain_servers_within_latency_window<'a>(
        &self,
        suitable_servers: &mut Vec<&'a ServerDescription>,
    ) {
        let shortest_average_rtt = suitable_servers
            .iter()
            .filter_map(|server_desc| server_desc.average_round_trip_time_ms)
            .fold(Option::<f64>::None, |min, curr| match min {
                Some(prev) => Some(prev.min(curr)),
                None => Some(curr),
            });

        let local_threshold = self.local_threshold.unwrap_or(DEFAULT_LOCAL_THRESHOLD);

        let max_rtt_within_window =
            shortest_average_rtt.map(|rtt| rtt + local_threshold.as_millis() as f64);

        suitable_servers.retain(move |server_desc| {
            if let Some(server_rtt) = server_desc.average_round_trip_time_ms {
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
    ) -> Vec<&'a ServerDescription> {
        match read_preference {
            ReadPreference::Primary => self.servers_with_type(&[ServerType::RSPrimary]).collect(),
            ReadPreference::Secondary {
                ref tag_sets,
                max_staleness,
            } => self.suitable_servers_for_read_preference(
                &[ServerType::RSSecondary],
                tag_sets.as_ref(),
                *max_staleness,
            ),
            ReadPreference::PrimaryPreferred {
                ref tag_sets,
                max_staleness,
            } => match self.servers_with_type(&[ServerType::RSPrimary]).next() {
                Some(primary) => vec![primary],
                None => self.suitable_servers_for_read_preference(
                    &[ServerType::RSSecondary],
                    tag_sets.as_ref(),
                    *max_staleness,
                ),
            },
            ReadPreference::SecondaryPreferred {
                ref tag_sets,
                max_staleness,
            } => {
                let suitable_servers = self.suitable_servers_for_read_preference(
                    &[ServerType::RSSecondary],
                    tag_sets.as_ref(),
                    *max_staleness,
                );

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
            ),
        }
    }

    fn suitable_servers_for_read_preference<'a>(
        &'a self,
        types: &'a [ServerType],
        tag_sets: Option<&'a Vec<TagSet>>,
        _max_staleness: Option<Duration>,
    ) -> Vec<&'a ServerDescription> {
        let mut servers = self.servers_with_type(types).collect();

        if let Some(tag_sets) = tag_sets {
            filter_servers_by_tag_sets(&mut servers, tag_sets);
        }

        // TODO RUST-193: Max staleness

        servers
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
