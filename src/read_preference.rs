use std::{collections::HashMap, time::Duration};

/// Specifies how the driver should route a read operation to members of a replica set.
///
/// If applicable, `tag_sets` can be used to target specific nodes in a replica set, and
/// `max_staleness` specifies the maximum lag behind the primary that a secondary can be to remain
/// eligible for the operation.
#[derive(Clone, Debug, PartialEq)]
pub enum ReadPreference {
    /// Only route this operation to the primary.
    Primary,
    /// Only route this operation to a secondary.
    Secondary {
        tag_sets: Option<Vec<TagSet>>,
        max_staleness: Option<Duration>,
    },
    /// Route this operation to the primary if it's available, but fall back to the secondaries if
    /// not.
    PrimaryPreferred {
        tag_sets: Option<Vec<TagSet>>,
        max_staleness: Option<Duration>,
    },
    /// Route this operation to a secondary if one is available, but fall back to the primary if
    /// not.
    SecondaryPreferred {
        tag_sets: Option<Vec<TagSet>>,
        max_staleness: Option<Duration>,
    },
    /// Route this operation to the node with the least network latency regardless of whether it's
    /// the primary or a secondary.
    Nearest {
        tag_sets: Option<Vec<TagSet>>,
        max_staleness: Option<Duration>,
    },
}

pub type TagSet = HashMap<String, String>;
