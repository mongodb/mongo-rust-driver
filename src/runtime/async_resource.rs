use std::sync::mpsc;

use futures_util::FutureExt;

use crate::client::auth::oidc::BoxFuture;

use super::spawn_blocking;

#[async_trait::async_trait]
pub(crate) trait AsyncDrop {
    async fn async_drop(&mut self);
}

pub(crate) struct AsyncResource<T> where T: AsyncDrop + Send + 'static {
    value: Option<T>,
    sender: mpsc::Sender<DropMessage>,
}

impl<T: AsyncDrop + Send + 'static> AsyncResource<T> {
    pub(crate) fn new(value: T) -> Self {
        let (sender, receiver) = mpsc::channel();
        let handle = tokio::runtime::Handle::current();
        spawn_blocking(|| async_drop_thread(receiver, handle));
        Self {
            value: Some(value),
            sender,
        }
    }

    pub(crate) fn add_child<C: AsyncDrop + Send + 'static>(&self, value: C) -> AsyncResource<C> {
        AsyncResource { value: Some(value), sender: self.sender.clone() }
    }

    pub(crate) async fn terminate(self) {
        // TODO: needs to be able to terminate children
        // add_child spawns a task listening on a one-shot that has an Arc<> to the value?
    }
}

impl<T: AsyncDrop + Send + 'static> Drop for AsyncResource<T> {
    fn drop(&mut self) {
        if let Some(mut v) = self.value.take() {
            let fut = async move {
                v.async_drop().await;
            }.boxed();
            self.sender.send(fut).unwrap();
        }
    }
}

type DropMessage = BoxFuture<'static, ()>;

fn async_drop_thread(receiver: mpsc::Receiver<DropMessage>, runtime: tokio::runtime::Handle) {
    while let Ok(drop_fut) = receiver.recv() {
        runtime.block_on(drop_fut);
    }
}