use std::{collections::HashMap, fmt, time::Duration};

use crate::error::{ErrorKind, Result};

/// Specifies how the driver should route a read operation to members of a replica set.
///
/// If applicable, `tag_sets` can be used to target specific nodes in a replica set, and
/// `max_staleness` specifies the maximum lag behind the primary that a secondary can be to remain
/// eligible for the operation. The max staleness value maps to the `maxStalenessSeconds` MongoDB
/// option and will be sent to the server as an integer number of seconds.
///
/// See the [MongoDB docs](https://docs.mongodb.com/manual/core/read-preference) for more details.
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

impl ReadPreference {
    pub(crate) fn is_primary(&self) -> bool {
        match self {
            ReadPreference::Primary => true,
            _ => false,
        }
    }

    pub(crate) fn is_secondary_preferred(&self) -> bool {
        match self {
            ReadPreference::SecondaryPreferred { .. } => true,
            _ => false,
        }
    }

    pub(crate) fn with_tags(self, tag_sets: Vec<TagSet>) -> Result<Self> {
        let tag_sets = Some(tag_sets);

        let read_pref = match self {
            ReadPreference::Primary => bail!(ErrorKind::ArgumentError(
                "read preference tags can only be specified when a non-primary mode is specified"
                    .to_string()
            )),
            ReadPreference::Secondary { max_staleness, .. } => ReadPreference::Secondary {
                tag_sets,
                max_staleness,
            },
            ReadPreference::PrimaryPreferred { max_staleness, .. } => {
                ReadPreference::PrimaryPreferred {
                    tag_sets,
                    max_staleness,
                }
            }
            ReadPreference::SecondaryPreferred { max_staleness, .. } => {
                ReadPreference::SecondaryPreferred {
                    tag_sets,
                    max_staleness,
                }
            }
            ReadPreference::Nearest { max_staleness, .. } => ReadPreference::Nearest {
                tag_sets,
                max_staleness,
            },
        };

        Ok(read_pref)
    }
}

impl fmt::Display for ReadPreference {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ReadPreference::Primary => write!(fmt, "primary"),
            ReadPreference::Secondary { tag_sets, .. } => {
                write!(fmt, "secondary, tag sets {:?}", tag_sets)
            }
            ReadPreference::PrimaryPreferred { tag_sets, .. } => {
                write!(fmt, "primaryPreferred, tag sets {:?}", tag_sets)
            }
            ReadPreference::SecondaryPreferred { tag_sets, .. } => {
                write!(fmt, "secondaryPreferred, tag sets {:?}", tag_sets)
            }
            ReadPreference::Nearest { tag_sets, .. } => {
                write!(fmt, "nearest, tag sets {:?}", tag_sets)
            }
        }
    }
}
