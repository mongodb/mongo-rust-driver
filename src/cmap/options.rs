use std::{sync::Arc, time::Duration};

use derivative::Derivative;
use serde::{Deserialize, Deserializer};
use typed_builder::TypedBuilder;

use crate::{
    event::cmap::CmapEventHandler,
    options::{ClientOptions, TlsOptions},
};

/// Contains the options for creating a connection pool. While these options are specified at the
/// client-level, `ConnectionPoolOptions` is exposed for the purpose of CMAP event handling.
#[derive(Default, Deserialize, TypedBuilder, Derivative)]
#[derivative(Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionPoolOptions {
    /// The application name specified by the user. This is sent to the server as part of the
    /// handshake that each connection makes when it's created.
    #[builder(default)]
    pub app_name: Option<String>,

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

    /// The options specifying how a TLS connection should be configured. If `tls_options` is
    /// `None`, then TLS will not be used for the connections.
    ///
    /// The default is not to use TLS for connections.
    #[builder(default)]
    #[serde(skip)]
    pub tls_options: Option<TlsOptions>,

    #[derivative(Debug = "ignore", PartialEq = "ignore")]
    #[builder(default)]
    #[serde(skip)]
    pub event_handler: Option<Arc<dyn CmapEventHandler>>,
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

impl ConnectionPoolOptions {
    pub(crate) fn from_client_options(options: &ClientOptions) -> Self {
        Self::builder()
            .max_pool_size(options.max_pool_size)
            .min_pool_size(options.min_pool_size)
            .max_idle_time(options.max_idle_time)
            .wait_queue_timeout(options.wait_queue_timeout)
            .tls_options(options.tls_options.clone())
            .event_handler(options.cmap_event_handler.clone())
            .build()
    }
}
