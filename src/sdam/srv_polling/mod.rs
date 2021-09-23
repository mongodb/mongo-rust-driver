#[cfg(test)]
mod test;

use std::time::Duration;

use super::{
    description::topology::TopologyType,
    monitor::DEFAULT_HEARTBEAT_FREQUENCY,
    state::{Topology, WeakTopology},
};
use crate::{
    error::{Error, Result},
    options::ClientOptions,
    srv::{LookupHosts, SrvResolver},
    RUNTIME,
};

const MIN_RESCAN_SRV_INTERVAL: Duration = Duration::from_secs(60);

pub(crate) struct SrvPollingMonitor {
    initial_hostname: String,
    resolver: Option<SrvResolver>,
    topology: WeakTopology,
    rescan_interval: Duration,
    client_options: ClientOptions,
}

impl SrvPollingMonitor {
    pub(crate) fn new(topology: WeakTopology) -> Option<Self> {
        let mut client_options = topology.client_options().clone();

        let initial_info = match client_options.original_srv_info.take() {
            Some(info) => info,
            None => return None,
        };

        Some(Self {
            initial_hostname: initial_info.hostname,
            resolver: None,
            topology,
            rescan_interval: initial_info.min_ttl,
            client_options,
        })
    }

    /// Starts a monitoring task that periodically performs SRV record lookups to determine if the
    /// set of mongos in the cluster have changed. A weak reference is used to ensure that the
    /// monitoring task doesn't keep the topology alive after the client has been dropped.
    pub(super) fn start(topology: WeakTopology) {
        RUNTIME.execute(async move {
            if let Some(mut monitor) = Self::new(topology) {
                monitor.execute().await;
            }
        });
    }

    fn rescan_interval(&self) -> Duration {
        std::cmp::max(self.rescan_interval, MIN_RESCAN_SRV_INTERVAL)
    }

    async fn execute(&mut self) {
        fn should_poll(tt: TopologyType) -> bool {
            matches!(tt, TopologyType::Sharded | TopologyType::Unknown)
        }

        while self.topology.is_alive() {
            RUNTIME.delay_for(self.rescan_interval()).await;

            let topology = match self.topology.upgrade() {
                Some(topology) => topology,
                None => break,
            };

            if should_poll(topology.topology_type().await) {
                let hosts = self.lookup_hosts().await;

                // verify we should still update before updating in case the topology changed
                // while the srv lookup was happening.
                if should_poll(topology.topology_type().await) {
                    self.update_hosts(hosts, topology.clone()).await;
                }
            }

            std::mem::drop(topology);
        }
    }

    async fn update_hosts(&mut self, lookup: Result<LookupHosts>, topology: Topology) {
        let lookup = match lookup {
            Ok(LookupHosts { hosts, .. }) if hosts.is_empty() => {
                self.no_valid_hosts(None);

                return;
            }
            Ok(lookup) => lookup,
            Err(err) => {
                self.no_valid_hosts(Some(err));

                return;
            }
        };

        self.rescan_interval = lookup.min_ttl;

        // TODO: RUST-230 Log error with host that was returned.
        topology
            .update_hosts(
                lookup.hosts.into_iter().filter_map(Result::ok).collect(),
                &self.client_options,
            )
            .await;
    }

    async fn lookup_hosts(&mut self) -> Result<LookupHosts> {
        #[cfg(test)]
        if let Some(mock) = self.client_options.test_options.as_ref().and_then(|to| to.mock_lookup_hosts.as_ref()) {
            return mock.clone();
        }
        let initial_hostname = self.initial_hostname.clone();
        let resolver = self.get_or_create_srv_resolver().await?;
        resolver.get_srv_hosts(initial_hostname.as_str()).await
    }

    async fn get_or_create_srv_resolver(&mut self) -> Result<&SrvResolver> {
        if let Some(ref resolver) = self.resolver {
            return Ok(resolver);
        }

        let resolver =
            SrvResolver::new(self.client_options.resolver_config.clone().map(|c| c.inner)).await?;

        // Since the connection was not `Some` above, this will always insert the new connection and
        // return a reference to it.
        Ok(self.resolver.get_or_insert(resolver))
    }

    fn no_valid_hosts(&mut self, _error: Option<Error>) {
        // TODO RUST-230: Log error/lack of valid results.

        self.rescan_interval = self.heartbeat_freq();
    }

    fn heartbeat_freq(&self) -> Duration {
        self.client_options
            .heartbeat_freq
            .unwrap_or(DEFAULT_HEARTBEAT_FREQUENCY)
    }
}
