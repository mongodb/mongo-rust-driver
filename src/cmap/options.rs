use std::time::Duration;

use serde::{Deserialize, Deserializer};

/// Contains the options for creating a connection pool. While these options are specified at the
/// client-level, `ConnectionPoolOptions` is exposed for the purpose of CMAP event handling.
#[derive(Debug, Deserialize, TypedBuilder, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionPoolOptions {
    /// The maximum number of connections that the pool can have at a given time. This includes
    /// connections which are currently checked out of the pool.
    ///
    /// The default is 100.
    #[builder(default)]
    pub max_pool_size: Option<u32>,

    /// The minimum number of connections that the pool can have at a given time. This includes
    /// connections which are currently checked out of the pool. If fewer than `min_pool_size`
    /// connections are in the pool, connections will be added to the pool in the background.
    ///
    /// The default is that no minimum is enforced
    #[builder(default)]
    pub min_pool_size: Option<u32>,

    /// Connections that have been ready for usage in the pool for longer than `max_idle_time` will
    /// not be used.
    ///
    /// The default is that connections will not be closed due to being idle.
    #[builder(default)]
    #[serde(rename = "maxIdleTimeMS")]
    #[serde(default)]
    #[serde(deserialize_with = "self::deserialize_duration_from_u64_millis")]
    pub max_idle_time: Option<Duration>,

    /// Rather than wait indefinitely for a connection to become available, instead return an error
    /// after the given duration.
    ///
    /// The default is to block indefinitely until a connection becomes available.
    #[builder(default)]
    #[serde(rename = "waitQueueTimeoutMS")]
    #[serde(default)]
    #[serde(deserialize_with = "self::deserialize_duration_from_u64_millis")]
    pub wait_queue_timeout: Option<Duration>,
}

fn deserialize_duration_from_u64_millis<'de, D>(
    deserializer: D,
) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let millis = Option::<u64>::deserialize(deserializer)?;
    Ok(millis.map(Duration::from_millis))
}
