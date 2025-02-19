use crate::action::action_impl;

#[action_impl]
impl<'a> Action for crate::action::WarmConnectionPool<'a> {
    type Future = WarmConnectionPoolFuture;

    async fn execute(self) -> () {
        if !self
            .client
            .inner
            .options
            .min_pool_size
            .is_some_and(|size| size > 0)
        {
            // No-op when min_pool_size is zero.
            return;
        }
        self.client.inner.topology.warm_pool().await;
    }
}
