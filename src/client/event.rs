use async_stream::stream;
use futures_core::Stream;
use tokio::sync::broadcast;

use crate::{event::command::CommandEvent, Client, error::{Result, ErrorKind}};

#[derive(Debug, Default)]
pub(super) struct Broadcast {
    command: std::sync::RwLock<Option<broadcast::Sender<CommandEvent>>>,
}

impl Broadcast {
    pub(super) fn command_is_receiving(&self) -> bool {
        self.command.read().unwrap().is_some()
    }

    pub(super) fn send_command(&self, ev: CommandEvent) -> bool {
        match self.command.read().unwrap().as_ref() {
            None => false,
            Some(sender) => sender.send(ev).is_ok(),
        }
    }
}

const CHANNEL_BUFFER: usize = 100;

impl Client {
    pub fn command_events(&self) -> impl Stream<Item=Result<CommandEvent>> + Send {
        let read_lock = self.inner.broadcast.command.read().unwrap();
        let receiver = match read_lock.as_ref() {
            Some(s) => s.subscribe(),
            None => {
                drop(read_lock);
                let mut write_lock = self.inner.broadcast.command.write().unwrap();
                match write_lock.as_ref() {
                    Some(s) => s.subscribe(),
                    None => {
                        let (sender, receiver) = broadcast::channel(CHANNEL_BUFFER);
                        *write_lock = Some(sender);
                        receiver
                    }
                }
            },
        };
        stream! {
            let mut receiver = receiver;
            loop {
                match receiver.recv().await {
                    Ok(ev) => yield Ok(ev),
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(n)) => yield Err(ErrorKind::EventLagged(n).into()),
                }
            }
        }
    }
}