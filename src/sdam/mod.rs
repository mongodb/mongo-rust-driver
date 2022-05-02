mod description;
mod monitor;
pub mod public;
mod server;
mod srv_polling;
#[cfg(test)]
mod test;
mod topology;

pub use self::public::{ServerInfo, ServerType, TopologyType};

pub(crate) use self::{
    description::{
        server::ServerDescription,
        topology::{
            server_selection::{self, SelectedServer, IDLE_WRITE_PERIOD},
            SessionSupportStatus,
            TopologyDescription,
            TransactionSupportStatus,
        },
    },
    monitor::{Monitor, DEFAULT_HEARTBEAT_FREQUENCY, MIN_HEARTBEAT_FREQUENCY},
    server::Server,
    topology::{
        HandshakePhase,
        Topology,
        TopologyCheckRequestReceiver,
        TopologyUpdater,
        TopologyWatcher,
    },
};

#[cfg(test)]
pub(crate) use topology::UpdateMessage;
