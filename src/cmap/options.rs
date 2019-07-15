use std::time::Duration;

#[derive(Debug, TypedBuilder)]
pub struct ConnectionPoolOptions {
    #[builder(default)]
    pub max_pool_size: Option<u32>,

    #[builder(default)]
    pub min_pool_size: Option<u32>,

    #[builder(default)]
    pub max_idle_time: Option<Duration>,

    #[builder(default)]
    pub wait_queue_timeout: Option<Duration>,
}
