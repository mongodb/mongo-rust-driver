use std::{collections::HashMap, sync::Arc, time::Duration};

use derivative::Derivative;
use serde::{de::Error as SerdeError, Deserialize, Deserializer, Serialize};
use typed_builder::TypedBuilder;

use crate::{
    bson::doc,
    error::{ErrorKind, Result},
    options::ServerAddress,
    sdam::public::ServerInfo,
    serde_util,
};

/// Describes which servers are suitable for a given operation.
#[derive(Clone, Derivative, derive_more::Display)]
#[derivative(Debug)]
#[non_exhaustive]
pub enum SelectionCriteria {
    /// A read preference that describes the suitable servers based on the server type, max
    /// staleness, and server tags.
    ///
    /// See the documentation [here](https://www.mongodb.com/docs/manual/core/read-preference/) for more details.
    #[display(fmt = "ReadPreference {}", _0)]
    ReadPreference(ReadPreference),

    /// A predicate used to filter servers that are considered suitable. A `server` will be
    /// considered suitable by a `predicate` if `predicate(server)` returns true.
    #[display(fmt = "Custom predicate")]
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

    pub(crate) fn from_address(address: ServerAddress) -> Self {
        SelectionCriteria::Predicate(Arc::new(move |server| server.address() == &address))
    }

    #[cfg(test)]
    pub(crate) fn serialize_for_client_options<S>(
        selection_criteria: &Option<SelectionCriteria>,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match selection_criteria {
            Some(SelectionCriteria::ReadPreference(pref)) => {
                ReadPreference::serialize_for_client_options(pref, serializer)
            }
            _ => serializer.serialize_none(),
        }
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
/// See the [MongoDB docs](https://www.mongodb.com/docs/manual/core/read-preference) for more details.
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

impl std::fmt::Display for ReadPreference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ Mode: ")?;
        let opts_ref = match self {
            ReadPreference::Primary => {
                write!(f, "Primary")?;
                None
            }
            ReadPreference::Secondary { options } => {
                write!(f, "Secondary")?;
                Some(options)
            }
            ReadPreference::PrimaryPreferred { options } => {
                write!(f, "PrimaryPreferred")?;
                Some(options)
            }
            ReadPreference::SecondaryPreferred { options } => {
                write!(f, "SecondaryPreferred")?;
                Some(options)
            }
            ReadPreference::Nearest { options } => {
                write!(f, "Nearest")?;
                Some(options)
            }
        };
        if let Some(opts) = opts_ref {
            if !opts.is_default() {
                if let Some(ref tag_sets) = opts.tag_sets {
                    write!(f, ", Tag Sets: {:?}", tag_sets)?;
                }
                if let Some(ref max_staleness) = opts.max_staleness {
                    write!(f, ", Max Staleness: {:?}", max_staleness)?;
                }
                if let Some(ref hedge) = opts.hedge {
                    write!(f, ", Hedge: {}", hedge.enabled)?;
                }
            }
        }
        write!(f, " }}")
    }
}

impl<'de> Deserialize<'de> for ReadPreference {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Serialize, Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct ReadPreferenceHelper {
            mode: String,
            #[serde(flatten)]
            options: ReadPreferenceOptions,
        }
        let preference = ReadPreferenceHelper::deserialize(deserializer)?;
        match preference.mode.to_ascii_lowercase().as_str() {
            "primary" => {
                if !preference.options.is_default() {
                    return Err(D::Error::custom(&format!(
                        "no options can be specified with read preference mode = primary, but got \
                         {:?}",
                        preference.options
                    )));
                }
                Ok(ReadPreference::Primary)
            }
            "secondary" => Ok(ReadPreference::Secondary {
                options: preference.options,
            }),
            "primarypreferred" => Ok(ReadPreference::PrimaryPreferred {
                options: preference.options,
            }),
            "secondarypreferred" => Ok(ReadPreference::SecondaryPreferred {
                options: preference.options,
            }),
            "nearest" => Ok(ReadPreference::Nearest {
                options: preference.options,
            }),
            other => Err(D::Error::custom(format!(
                "Unknown read preference mode: {}",
                other
            ))),
        }
    }
}

impl Serialize for ReadPreference {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[serde_with::skip_serializing_none]
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct ReadPreferenceHelper<'a> {
            mode: &'static str,
            #[serde(flatten)]
            options: Option<&'a ReadPreferenceOptions>,
        }
        let helper = match self {
            ReadPreference::Primary => ReadPreferenceHelper {
                mode: "primary",
                options: None,
            },
            ReadPreference::PrimaryPreferred { options } => ReadPreferenceHelper {
                mode: "primaryPreferred",
                options: Some(options),
            },
            ReadPreference::Secondary { options } => ReadPreferenceHelper {
                mode: "secondary",
                options: Some(options),
            },
            ReadPreference::SecondaryPreferred { options } => ReadPreferenceHelper {
                mode: "secondaryPreferred",
                options: Some(options),
            },
            ReadPreference::Nearest { options } => ReadPreferenceHelper {
                mode: "nearest",
                options: Some(options),
            },
        };

        helper.serialize(serializer)
    }
}

/// Specifies read preference options for non-primary read preferences.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
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
        default,
        with = "serde_util::duration_option_as_int_seconds"
    )]
    pub max_staleness: Option<Duration>,

    /// Specifies hedging behavior for reads. These options only apply to sharded clusters on
    /// servers that are at least version 4.4. Note that hedged reads are automatically enabled for
    /// read preference mode "nearest".
    ///
    /// See the [MongoDB docs](https://www.mongodb.com/docs/manual/core/read-preference-hedge-option/) for more details.
    pub hedge: Option<HedgedReadOptions>,
}

impl ReadPreferenceOptions {
    pub(crate) fn is_default(&self) -> bool {
        self.hedge.is_none()
            && self.max_staleness.is_none()
            && self
                .tag_sets
                .as_ref()
                .map(|ts| ts.is_empty() || ts[..] == [HashMap::default()])
                .unwrap_or(true)
    }
}

/// Specifies hedging behavior for reads.
///
/// See the [MongoDB docs](https://www.mongodb.com/docs/manual/core/read-preference-hedge-option/) for more details.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, TypedBuilder)]
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
                return Err(ErrorKind::InvalidArgument {
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
                return Err(ErrorKind::InvalidArgument {
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

    #[cfg(test)]
    pub(crate) fn serialize_for_client_options<S>(
        read_preference: &ReadPreference,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(serde::Serialize)]
        struct ReadPreferenceHelper<'a> {
            readpreference: &'a str,

            readpreferencetags: Option<&'a Vec<HashMap<String, String>>>,

            #[serde(serialize_with = "serde_util::duration_option_as_int_seconds::serialize")]
            maxstalenessseconds: Option<Duration>,
        }

        let state = match read_preference {
            ReadPreference::Primary => ReadPreferenceHelper {
                readpreference: "primary",
                readpreferencetags: None,
                maxstalenessseconds: None,
            },
            ReadPreference::PrimaryPreferred { options } => ReadPreferenceHelper {
                readpreference: "primaryPreferred",
                readpreferencetags: options.tag_sets.as_ref(),
                maxstalenessseconds: options.max_staleness,
            },
            ReadPreference::Secondary { options } => ReadPreferenceHelper {
                readpreference: "secondary",
                readpreferencetags: options.tag_sets.as_ref(),
                maxstalenessseconds: options.max_staleness,
            },
            ReadPreference::SecondaryPreferred { options } => ReadPreferenceHelper {
                readpreference: "secondaryPreferred",
                readpreferencetags: options.tag_sets.as_ref(),
                maxstalenessseconds: options.max_staleness,
            },
            ReadPreference::Nearest { options } => ReadPreferenceHelper {
                readpreference: "nearest",
                readpreferencetags: options.tag_sets.as_ref(),
                maxstalenessseconds: options.max_staleness,
            },
        };

        state.serialize(serializer)
    }
}

/// A read preference tag set. See the documentation [here](https://www.mongodb.com/docs/manual/tutorial/configure-replica-set-tag-sets/) for more details.
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
        let doc = bson::to_document(&read_pref).unwrap();

        assert_eq!(
            doc,
            doc! { "mode": "secondary", "hedge": { "enabled": true } }
        );
    }
}
