use std::{collections::HashMap, sync::Arc, time::Duration};

use derivative::Derivative;
use typed_builder::TypedBuilder;

use crate::{
    bson::{doc, Bson, Document},
    error::{ErrorKind, Result},
    options::StreamAddress,
    sdam::public::ServerInfo,
};

/// Describes which servers are suitable for a given operation.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
#[non_exhaustive]
pub enum SelectionCriteria {
    /// A read preference that describes the suitable servers based on the server type, max
    /// staleness, and server tags.
    ///
    /// See the documentation [here](https://docs.mongodb.com/manual/core/read-preference/) for more details.
    ReadPreference(ReadPreference),

    /// A predicate used to filter servers that are considered suitable. A `server` will be
    /// considered suitable by a `predicate` if `predicate(server)` returns true.
    Predicate(#[derivative(Debug = "ignore")] Predicate),
}

impl PartialEq for SelectionCriteria {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::ReadPreference(r1), Self::ReadPreference(r2)) => r1 == r2,
            _ => false,
        }
    }
}

impl From<ReadPreference> for SelectionCriteria {
    fn from(read_pref: ReadPreference) -> Self {
        Self::ReadPreference(read_pref)
    }
}

impl SelectionCriteria {
    pub(crate) fn as_read_pref(&self) -> Option<&ReadPreference> {
        match self {
            Self::ReadPreference(ref read_pref) => Some(read_pref),
            Self::Predicate(..) => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn as_predicate(&self) -> Option<&Predicate> {
        match self {
            Self::Predicate(ref p) => Some(p),
            _ => None,
        }
    }

    pub(crate) fn is_read_pref_primary(&self) -> bool {
        match self {
            Self::ReadPreference(ReadPreference::Primary) => true,
            _ => false,
        }
    }

    pub(crate) fn max_staleness(&self) -> Option<Duration> {
        self.as_read_pref().and_then(|pref| pref.max_staleness())
    }

    pub(crate) fn from_address(address: StreamAddress) -> Self {
        SelectionCriteria::Predicate(Arc::new(move |server| server.address() == &address))
    }
}

/// A predicate used to filter servers that are considered suitable.
pub type Predicate = Arc<dyn Send + Sync + Fn(&ServerInfo) -> bool>;

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
    Secondary { options: ReadPreferenceOptions },
    /// Route this operation to the primary if it's available, but fall back to the secondaries if
    /// not.
    PrimaryPreferred { options: ReadPreferenceOptions },

    /// Route this operation to a secondary if one is available, but fall back to the primary if
    /// not.
    SecondaryPreferred { options: ReadPreferenceOptions },

    /// Route this operation to the node with the least network latency regardless of whether it's
    /// the primary or a secondary.
    Nearest { options: ReadPreferenceOptions },
}

/// Specifies read preference options for non-primary read preferences.
#[derive(Clone, Debug, Default, PartialEq, TypedBuilder)]
#[non_exhaustive]
pub struct ReadPreferenceOptions {
    /// Specifies which replica set members should be considered for operations. Each tag set will
    /// be checked in order until one or more servers is found with each tag in the set.
    #[builder(default)]
    pub tag_sets: Option<Vec<TagSet>>,

    /// Specifies the maximum amount of lag behind the primary that a secondary can be to be
    /// considered for the given operation. Any secondaries lagging behind more than
    /// `max_staleness` will not be considered for the operation.
    ///
    /// `max_stalesness` must be at least 90 seconds. If a `max_stalness` less than 90 seconds is
    /// specified for an operation, the operation will return an error.
    #[builder(default)]
    pub max_staleness: Option<Duration>,
}

impl ReadPreference {
    pub(crate) fn max_staleness(&self) -> Option<Duration> {
        match self {
            ReadPreference::Primary => None,
            ReadPreference::Secondary { ref options }
            | ReadPreference::PrimaryPreferred { ref options }
            | ReadPreference::SecondaryPreferred { ref options }
            | ReadPreference::Nearest { ref options } => options.max_staleness,
        }
    }

    pub(crate) fn with_tags(mut self, tag_sets: Vec<TagSet>) -> Result<Self> {
        let options = match self {
            ReadPreference::Primary => {
                return Err(ErrorKind::ArgumentError {
                    message: "read preference tags can only be specified when a non-primary mode \
                              is specified"
                        .to_string(),
                }
                .into());
            }
            ReadPreference::Secondary { ref mut options } => options,
            ReadPreference::PrimaryPreferred { ref mut options } => options,
            ReadPreference::SecondaryPreferred { ref mut options } => options,
            ReadPreference::Nearest { ref mut options } => options,
        };

        options.tag_sets = Some(tag_sets);

        Ok(self)
    }

    pub(crate) fn with_max_staleness(mut self, max_staleness: Duration) -> Result<Self> {
        let options = match self {
            ReadPreference::Primary => {
                return Err(ErrorKind::ArgumentError {
                    message: "max staleness can only be specified when a non-primary mode is \
                              specified"
                        .to_string(),
                }
                .into());
            }
            ReadPreference::Secondary { ref mut options } => options,
            ReadPreference::PrimaryPreferred { ref mut options } => options,
            ReadPreference::SecondaryPreferred { ref mut options } => options,
            ReadPreference::Nearest { ref mut options } => options,
        };

        options.max_staleness = Some(max_staleness);

        Ok(self)
    }

    pub(crate) fn into_document(self) -> Document {
        let (mode, tag_sets, max_staleness) = match self {
            ReadPreference::Primary => ("primary", None, None),
            ReadPreference::PrimaryPreferred { options } => {
                ("primaryPreferred", options.tag_sets, options.max_staleness)
            }
            ReadPreference::Secondary { options } => {
                ("secondary", options.tag_sets, options.max_staleness)
            }
            ReadPreference::SecondaryPreferred { options } => (
                "secondaryPreferred",
                options.tag_sets,
                options.max_staleness,
            ),
            ReadPreference::Nearest { options } => {
                ("nearest", options.tag_sets, options.max_staleness)
            }
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

/// A read preference tag set. See the documentation [here](https://docs.mongodb.com/manual/tutorial/configure-replica-set-tag-sets/) for more details.
pub type TagSet = HashMap<String, String>;
