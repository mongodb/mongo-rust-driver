//! Contains the events and functionality for monitoring internal `Client` behavior.

pub mod cmap;
pub mod command;
pub mod sdam;

use std::sync::Arc;

use futures_core::future::BoxFuture;

use crate::event::command::CommandEvent;

/// A destination for events.  Allows implicit conversion via [`From`] for concrete types for
/// convenience with [`crate::options::ClientOptions`] construction:
///
/// ```rust
/// # use mongodb::options::ClientOptions;
/// # fn example() {
/// let (tx, mut rx) = tokio::sync::mpsc::channel(100);
/// tokio::spawn(async move {
///     while let Some(ev) = rx.recv().await {
///         println!("{:?}", ev);
///     }
/// });
/// let options = ClientOptions::builder()
///                 .command_event_handler(tx)
///                 .build();
/// # }
/// ```
///
/// or explicit construction for `Fn` traits:
///
/// ```rust
/// # use mongodb::options::ClientOptions;
/// # use mongodb::event::EventHandler;
/// # fn example() {
/// let options = ClientOptions::builder()
///                 .command_event_handler(EventHandler::callback(|ev| println!("{:?}", ev)))
///                 .build();
/// # }
/// ```
#[derive(Clone)]
#[non_exhaustive]
pub enum EventHandler<T> {
    /// A callback.
    Callback(Arc<dyn Fn(T) + Sync + Send>),
    /// An async callback.
    AsyncCallback(Arc<dyn Fn(T) -> BoxFuture<'static, ()> + Sync + Send>),
    /// A `tokio` channel sender.
    TokioMpsc(tokio::sync::mpsc::Sender<T>),
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
        Self::callback(move |ev| match ev {
            CommandEvent::Started(e) => value.handle_command_started_event(e),
            CommandEvent::Succeeded(e) => value.handle_command_succeeded_event(e),
            CommandEvent::Failed(e) => value.handle_command_failed_event(e),
        })
    }
}

impl<T: Send + Sync + 'static> EventHandler<T> {
    /// Construct a new event handler with a callback.
    pub fn callback(f: impl Fn(T) + Send + Sync + 'static) -> Self {
        Self::Callback(Arc::new(f))
    }

    /// Construct a new event handler with an async callback.
    pub fn async_callback(f: impl Fn(T) -> BoxFuture<'static, ()> + Send + Sync + 'static) -> Self {
        Self::AsyncCallback(Arc::new(f))
    }

    pub(crate) fn handle(&self, event: T) {
        match self {
            // TODO RUST-1731 Use tokio's spawn_blocking
            Self::Callback(cb) => (cb)(event),
            Self::AsyncCallback(cb) => {
                crate::runtime::spawn((cb)(event));
            }
            Self::TokioMpsc(sender) => {
                let sender = sender.clone();
                crate::runtime::spawn(async move {
                    let _ = sender.send(event).await;
                });
            }
        }
    }
}
