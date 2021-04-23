use std::{collections::HashMap, sync::Arc, time::Duration};

use derivative::Derivative;
use serde::{de::Error, Deserialize, Deserializer};
use typed_builder::TypedBuilder;

use crate::{
    bson::{doc, Bson, Document},
    bson_util::deserialize_duration_from_u64_seconds,
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

    #[cfg(test)]
    pub(crate) fn is_read_pref_primary(&self) -> bool {
        matches!(self, Self::ReadPreference(ReadPreference::Primary))
    }

    pub(crate) fn max_staleness(&self) -> Option<Duration> {
        self.as_read_pref().and_then(|pref| pref.max_staleness())
    }

    pub(crate) fn from_address(address: StreamAddress) -> Self {
        SelectionCriteria::Predicate(Arc::new(move |server| server.address() == &address))
    }
}

impl<'de> Deserialize<'de> for SelectionCriteria {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(SelectionCriteria::ReadPreference(
            ReadPreference::deserialize(deserializer)?,
        ))
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
#[allow(missing_docs)]
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

impl<'de> Deserialize<'de> for ReadPreference {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct ReadPreferenceHelper {
            mode: String,
            #[serde(flatten)]
            options: ReadPreferenceOptions,
        }
        let preference = ReadPreferenceHelper::deserialize(deserializer)?;

        match preference.mode.as_str() {
            "Primary" => Ok(ReadPreference::Primary),
            "Secondary" => Ok(ReadPreference::Secondary {
                options: preference.options,
            }),
            "PrimaryPreferred" => Ok(ReadPreference::PrimaryPreferred {
                options: preference.options,
            }),
            "SecondaryPreferred" => Ok(ReadPreference::SecondaryPreferred {
                options: preference.options,
            }),
            "Nearest" => Ok(ReadPreference::Nearest {
                options: preference.options,
            }),
            other => Err(D::Error::custom(format!(
                "Unknown read preference mode: {}",
                other
            ))),
        }
    }
}

/// Specifies read preference options for non-primary read preferences.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(strip_option)))]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ReadPreferenceOptions {
    /// Specifies which replica set members should be considered for operations. Each tag set will
    /// be checked in order until one or more servers is found with each tag in the set.
    pub tag_sets: Option<Vec<TagSet>>,

    /// Specifies the maximum amount of lag behind the primary that a secondary can be to be
    /// considered for the given operation. Any secondaries lagging behind more than
    /// `max_staleness` will not be considered for the operation.
    ///
    /// `max_staleness` must be at least 90 seconds. If a `max_staleness` less than 90 seconds is
    /// specified for an operation, the operation will return an error.
    #[serde(
        rename = "maxStalenessSeconds",
        deserialize_with = "deserialize_duration_from_u64_seconds"
    )]
    pub max_staleness: Option<Duration>,

    /// Specifies hedging behavior for reads. These options only apply to sharded clusters on
    /// servers that are at least version 4.4. Note that hedged reads are automatically enabled for
    /// read preference mode "nearest".
    ///
    /// See the [MongoDB docs](https://docs.mongodb.com/manual/core/read-preference-hedge-option/) for more details.
    pub hedge: Option<HedgedReadOptions>,
}

/// Specifies hedging behavior for reads.
///
/// See the [MongoDB docs](https://docs.mongodb.com/manual/core/read-preference-hedge-option/) for more details.
#[derive(Clone, Debug, Deserialize, PartialEq, TypedBuilder)]
#[non_exhaustive]
pub struct HedgedReadOptions {
    /// Whether or not to allow reads from a sharded cluster to be "hedged" across two replica
    /// set members per shard, with the results from the first response received back from either
    /// being returned.
    pub enabled: bool,
}

impl HedgedReadOptions {
    /// Creates a new `HedgedReadOptions` with the given value for `enabled`.
    pub fn with_enabled(enabled: bool) -> Self {
        Self { enabled }
    }
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
        let (mode, tag_sets, max_staleness, hedge) = match self {
            ReadPreference::Primary => ("primary", None, None, None),
            ReadPreference::PrimaryPreferred { options } => (
                "primaryPreferred",
                options.tag_sets,
                options.max_staleness,
                options.hedge,
            ),
            ReadPreference::Secondary { options } => (
                "secondary",
                options.tag_sets,
                options.max_staleness,
                options.hedge,
            ),
            ReadPreference::SecondaryPreferred { options } => (
                "secondaryPreferred",
                options.tag_sets,
                options.max_staleness,
                options.hedge,
            ),
            ReadPreference::Nearest { options } => (
                "nearest",
                options.tag_sets,
                options.max_staleness,
                options.hedge,
            ),
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

        if let Some(hedge) = hedge {
            doc.insert("hedge", doc! { "enabled": hedge.enabled });
        }

        doc
    }
}

/// A read preference tag set. See the documentation [here](https://docs.mongodb.com/manual/tutorial/configure-replica-set-tag-sets/) for more details.
pub type TagSet = HashMap<String, String>;

#[cfg(test)]
mod test {
    use super::{HedgedReadOptions, ReadPreference, ReadPreferenceOptions};
    use crate::bson::doc;

    #[test]
    fn hedged_read_included_in_document() {
        let options = ReadPreferenceOptions::builder()
            .hedge(HedgedReadOptions { enabled: true })
            .build();

        let read_pref = ReadPreference::Secondary { options };
        let doc = read_pref.into_document();

        assert_eq!(
            doc,
            doc! { "mode": "secondary", "hedge": { "enabled": true } }
        );
    }
}
