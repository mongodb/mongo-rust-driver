use rand::seq::IteratorRandom;

use super::TopologyDescription;
use crate::{read_preference::ReadPreference, sdam::description::server::ServerDescription};

/// Describes which servers are suitable for a given operation.
pub(crate) enum SelectionCriteria {
    /// A read preference that describes the suitable servers based on the server type, max
    /// staleness, and server tags.
    ReadPreference(ReadPreference),
    /// A predicate used to filter servers that are considered suitable. A `server` will be
    /// considered suitable by a `predicate` if `predicate(server)` returns true.
    Predicate(Box<dyn Fn(&ServerDescription) -> bool>),
}

impl TopologyDescription {
    /// Selects a server for an operation given criteria.
    pub(crate) fn select_server(&self, selector: SelectionCriteria) -> Option<&ServerDescription> {
        self.suitable_servers(selector)
            .into_iter()
            .choose(&mut rand::thread_rng())
    }

    /// Gets all of the suitable servers for an operation given criteria.
    fn suitable_servers(&self, selector: SelectionCriteria) -> Vec<&ServerDescription> {
        unimplemented!()
    }
}
