use std::future::IntoFuture;

use futures_util::FutureExt;

use crate::BoxFuture;

impl<'a> IntoFuture for crate::action::WarmConnectionPool<'a> {
    type Output = ();
    type IntoFuture = BoxFuture<'a, ()>;

    fn into_future(self) -> Self::IntoFuture {
        async {
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
        .boxed()
    }
}
