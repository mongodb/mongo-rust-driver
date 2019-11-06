use derivative::Derivative;
use rand::seq::IteratorRandom;

use super::TopologyDescription;
use crate::{read_preference::ReadPreference, sdam::description::server::ServerDescription};

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

impl TopologyDescription {
    /// Selects a server for an operation given criteria.
    pub(crate) fn select_server(&self, criteria: &SelectionCriteria) -> Option<&ServerDescription> {
        self.suitable_servers(criteria)
            .into_iter()
            .choose(&mut rand::thread_rng())
    }

    /// Gets all of the suitable servers for an operation given criteria.
    fn suitable_servers(&self, selector: &SelectionCriteria) -> Vec<&ServerDescription> {
        unimplemented!()
    }
}
