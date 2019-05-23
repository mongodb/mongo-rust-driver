use std::{
    sync::{Arc, RwLock, Weak},
    thread,
    time::Duration,
};

use super::Server;
use crate::topology::Topology;

lazy_static! {
    static ref DEFAULT_HEARTBEAT_FREQUENCY: Duration = Duration::from_secs(2);
}

pub(crate) fn start_monitor(
    server: Weak<RwLock<Server>>,
    topology: Weak<RwLock<Topology>>,
    heartbeat_freq: Option<Duration>,
) {
    macro_rules! get_ref_or_break {
        ($weak:expr) => {
            match $weak.upgrade() {
                Some(t) => t,
                None => return,
            }
        };
    }

    thread::spawn(move || loop {
        let address = get_ref_or_break!(server).read().unwrap().address();

        let server_type = match get_ref_or_break!(topology)
            .read()
            .unwrap()
            .get_server_type(&address)
        {
            Some(t) => t,
            None => return,
        };

        let server_description = get_ref_or_break!(server)
            .write()
            .unwrap()
            .check(server_type);
        let mut topology_description = get_ref_or_break!(topology)
            .read()
            .unwrap()
            .description
            .clone();
        topology_description.update(server_description);

        for new_server in get_ref_or_break!(topology)
            .write()
            .unwrap()
            .update_description(topology_description)
        {
            start_monitor(
                new_server,
                Arc::downgrade(&get_ref_or_break!(topology)),
                heartbeat_freq,
            );
        }

        thread::sleep(heartbeat_freq.unwrap_or(*DEFAULT_HEARTBEAT_FREQUENCY));
    });
}
