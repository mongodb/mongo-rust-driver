use std::time::Duration;

use serde::{Deserialize, Deserializer};

#[derive(Debug, Deserialize, TypedBuilder, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionPoolOptions {
    #[builder(default)]
    pub max_pool_size: Option<u32>,

    #[builder(default)]
    pub min_pool_size: Option<u32>,

    #[builder(default)]
    #[serde(rename = "maxIdleTimeMS")]
    #[serde(default)]
    #[serde(deserialize_with = "self::deserialize_duration_from_millis")]
    pub max_idle_time: Option<Duration>,

    #[builder(default)]
    #[serde(rename = "waitQueueTimeoutMS")]
    #[serde(default)]
    #[serde(deserialize_with = "self::deserialize_duration_from_millis")]
    pub wait_queue_timeout: Option<Duration>,
}

impl ConnectionPoolOptions {
    pub(crate) fn is_empty(&self) -> bool {
        self.max_pool_size.is_none()
            && self.min_pool_size.is_none()
            && self.max_idle_time.is_none()
            && self.wait_queue_timeout.is_none()
    }
}

fn deserialize_duration_from_millis<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let millis = Option::<u64>::deserialize(deserializer)?;
    Ok(millis.map(Duration::from_millis))
}
