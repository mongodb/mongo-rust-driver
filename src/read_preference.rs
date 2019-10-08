use std::{collections::HashMap, time::Duration};

use bson::{bson, doc, Bson, Document};

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

impl ReadPreference {
    pub(crate) fn with_tags(self, tag_sets: Vec<TagSet>) -> Result<Self> {
        let tag_sets = Some(tag_sets);

        let read_pref = match self {
            ReadPreference::Primary => {
                return Err(ErrorKind::ArgumentError {
                    message: "read preference tags can only be specified when a non-primary mode \
                              is specified"
                        .to_string(),
                }
                .into());
            }
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

    pub(crate) fn into_document(self) -> Document {
        let (mode, tag_sets, max_staleness) = match self.clone() {
            ReadPreference::Primary => ("primary", None, None),
            ReadPreference::PrimaryPreferred {
                tag_sets,
                max_staleness,
            } => ("primaryPreferred", tag_sets, max_staleness),
            ReadPreference::Secondary {
                tag_sets,
                max_staleness,
            } => ("secondary", tag_sets, max_staleness),
            ReadPreference::SecondaryPreferred {
                tag_sets,
                max_staleness,
            } => ("secondaryPreferred", tag_sets, max_staleness),
            ReadPreference::Nearest {
                tag_sets,
                max_staleness,
            } => ("nearest", tag_sets, max_staleness),
        };

        let mut doc = doc! { "mode": mode };

        if let Some(max_stale) = max_staleness {
            doc.insert("maxStalenessSeconds", max_stale.as_secs());
        }

        if let Some(tag_sets) = tag_sets {
            let tags: Vec<Bson> = tag_sets
                .into_iter()
                .map(|tag_set| {
                    Bson::Document(tag_set.into_iter().map(|(k, v)| (k, v.into())).collect())
                })
                .collect();
            doc.insert("tags", tags);
        }

        doc
    }
}

pub type TagSet = HashMap<String, String>;
