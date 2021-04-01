mod description;
#[allow(dead_code)]
mod message_manager;
mod monitor;
pub mod public;
mod srv_polling;
mod state;
#[cfg(test)]
mod test;

pub use self::public::{ServerInfo, ServerType};

#[cfg(test)]
pub(crate) use self::description::server::ServerDescription;
pub(crate) use self::{
    description::topology::{
        server_selection::SelectedServer,
        SessionSupportStatus,
        TopologyDescription,
    },
    message_manager::TopologyMessageManager,
    monitor::MIN_HEARTBEAT_FREQUENCY,
    state::{
        server::{Server, ServerUpdate, ServerUpdateReceiver, ServerUpdateSender},
        HandshakePhase,
        Topology,
    },
};
