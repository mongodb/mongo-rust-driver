use crate::action::action_execute;

action_execute! {
    crate::action::WarmConnectionPool<'a> => WarmConnectionPoolFuture;

    async fn(self) -> () {
        if !self
            .client
            .inner
            .options
            .min_pool_size
            .map_or(false, |s| s > 0)
        {
            // No-op when min_pool_size is zero.
            return;
        }
        self.client.inner.topology.warm_pool().await;
    }
}
