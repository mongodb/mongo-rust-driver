use std::sync::Arc;

use crate::event::command::CommandEvent;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum EventHandler<T> {
    TokioMpsc(tokio::sync::mpsc::Sender<T>),
    Legacy(LegacyEventHandler<T>),
}

impl<T: Send + Sync> From<tokio::sync::mpsc::Sender<T>> for EventHandler<T> {
    fn from(value: tokio::sync::mpsc::Sender<T>) -> Self {
        Self::TokioMpsc(value)
    }
}

#[allow(deprecated)]
impl<T: crate::event::command::CommandEventHandler + 'static> From<Arc<T>> for EventHandler<CommandEvent> {
    fn from(value: Arc<T>) -> Self {
        Self::Legacy(LegacyEventHandler(Arc::new(move |ev| match ev {
            CommandEvent::Started(e) => value.handle_command_started_event(e),
            CommandEvent::Succeeded(e) => value.handle_command_succeeded_event(e),
            CommandEvent::Failed(e) => value.handle_command_failed_event(e),
        })))
    }
}

impl<T> EventHandler<T> {
    pub(crate) async fn handle(&self, event: T) {
        match self {
            Self::TokioMpsc(sender) => {
                let _ = sender.send(event).await;
            }
            Self::Legacy(LegacyEventHandler(f)) => (f)(event),
        }
    }
}

#[derive(Clone)]
pub struct LegacyEventHandler<T>(Arc<dyn Fn(T) + Send + Sync>);

impl<T> std::fmt::Debug for LegacyEventHandler<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("LegacyEventHandler").finish()
    }
}