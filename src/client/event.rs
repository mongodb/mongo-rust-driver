use std::sync::Arc;

use futures_core::future::BoxFuture;

use crate::event::command::CommandEvent;

#[derive(Clone)]
#[non_exhaustive]
pub enum EventHandler<T> {
    Callback(Arc<dyn Fn(T) + Sync + Send>),
    AsyncCallback(Arc<dyn Fn(T) -> BoxFuture<'static, ()> + Sync + Send>),
    TokioMpsc(tokio::sync::mpsc::Sender<T>),
    Legacy(LegacyEventHandler<T>),
}

impl<T> std::fmt::Debug for EventHandler<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("EventHandler").finish()
    }
}

impl<T> From<tokio::sync::mpsc::Sender<T>> for EventHandler<T> {
    fn from(value: tokio::sync::mpsc::Sender<T>) -> Self {
        Self::TokioMpsc(value)
    }
}

#[allow(deprecated)]
impl<T: crate::event::command::CommandEventHandler + 'static> From<Arc<T>>
    for EventHandler<CommandEvent>
{
    fn from(value: Arc<T>) -> Self {
        Self::Legacy(LegacyEventHandler(Arc::new(move |ev| match ev {
            CommandEvent::Started(e) => value.handle_command_started_event(e),
            CommandEvent::Succeeded(e) => value.handle_command_succeeded_event(e),
            CommandEvent::Failed(e) => value.handle_command_failed_event(e),
        })))
    }
}

impl<T> EventHandler<T> {
    pub fn new_callback(f: impl Fn(T) -> BoxFuture<'static, ()> + Send + Sync + 'static) -> Self {
        Self::AsyncCallback(Arc::new(f))
    }

    pub fn new_sync_callback(f: impl Fn(T) + Send + Sync + 'static) -> Self {
        Self::Callback(Arc::new(f))
    }

    pub(crate) async fn handle(&self, event: T) {
        match self {
            // TODO RUST-1731 Use tokio's spawn_blocking
            Self::Callback(cb) => (cb)(event),
            Self::AsyncCallback(cb) => (cb)(event).await,
            Self::TokioMpsc(sender) => {
                let _ = sender.send(event).await;
            }
            Self::Legacy(LegacyEventHandler(f)) => (f)(event),
        }
    }
}

#[derive(Clone)]
pub struct LegacyEventHandler<T>(Arc<dyn Fn(T) + Send + Sync>);
