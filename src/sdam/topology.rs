use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::client::options::{ClientOptions, ServerAddress};
use crate::error::Result;

use super::{Server, TopologyDescription};

pub(crate) struct NewTopology {
    watcher: TopologyWatcher,
    updater: TopologyUpdater,
}

impl NewTopology {
    pub(crate) fn new(options: ClientOptions) -> Result<NewTopology> {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<UpdateMessage>();

        // let state
        let state = TopologyState {
            description: TopologyDescription::new(options)?,
            servers: HashMap::new(),
        };
        let (watch_sender, watch_receiver) = tokio::sync::watch::channel(state);


        todo!()
    }
}

pub(crate) struct TopologyState {
    description: TopologyDescription,
    servers: HashMap<ServerAddress, Arc<Server>>,
}

enum UpdateMessage {}

struct TopologyWorker {
    update_receiver: UnboundedReceiver<UpdateMessage>,
}

impl TopologyWorker {
    fn start() {}
}

pub(crate) struct TopologyUpdater {
    sender: UnboundedSender<UpdateMessage>,
}

pub(crate) struct TopologyWatcher {}
