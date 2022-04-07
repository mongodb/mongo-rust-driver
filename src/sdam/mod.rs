mod description;
#[allow(dead_code)]
mod message_manager;
mod monitor;
pub mod public;
mod srv_polling;
mod state;
#[cfg(test)]
mod test;
mod topology;

pub use self::public::{ServerInfo, ServerType, TopologyType};

pub(crate) use self::{
    description::{
        server::ServerDescription,
        topology::{
            server_selection::{self, SelectedServer},
            SessionSupportStatus,
            TopologyDescription,
            TransactionSupportStatus,
        },
    },
    message_manager::TopologyMessageManager,
    monitor::{HMonitor, MIN_HEARTBEAT_FREQUENCY},
    state::{
        server::{Server, ServerUpdate, ServerUpdateReceiver, ServerUpdateSender},
        HandshakePhase,
        Topology,
    },
    topology::{NewTopology, TopologyUpdateRequestReceiver, TopologyUpdater, TopologyWatcher},
};

#[cfg(test)]
pub(crate) use topology::UpdateMessage;
