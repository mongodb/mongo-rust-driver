use std::time::Duration;

use super::{
    description::topology::TopologyDescriptionDiff,
    monitor::DEFAULT_HEARTBEAT_FREQUENCY,
    state::{TopologyState, WeakTopology},
};
use crate::{
    error::{Error, Result},
    options::{ClientOptions, StreamAddress},
    srv::SrvResolver,
    RUNTIME,
};

const DEFAULT_RESCAN_SRV_INTERVAL: Duration = Duration::from_secs(60);

pub(super) struct SrvPollingMonitor {
    initial_hostname: String,
    resolver: Option<SrvResolver>,
    topology: WeakTopology,
    rescan_interval: Option<Duration>,
    client_options: ClientOptions,
}

impl SrvPollingMonitor {
    pub(super) fn start(topology: WeakTopology) {
        let client_options = topology.client_options().clone();

        let initial_hostname = match client_options.original_srv_hostname() {
            Some(hostname) => hostname.clone(),
            None => return,
        };

        RUNTIME.execute(async move {
            let mut monitor = Self {
                initial_hostname,
                resolver: None,
                topology,
                rescan_interval: None,
                client_options,
            };

            monitor.execute().await;
        });
    }

    async fn execute(&mut self) {
        while let Some(topology) = self.topology.upgrade() {
            let mut state = topology.clone_state().await;

            if state.is_sharded() || state.is_unknown() {
                let diff = self.update_hosts(&mut state).await;
                topology.update_state(diff, state).await;
            }

            RUNTIME
                .delay_for(self.rescan_interval.unwrap_or(DEFAULT_RESCAN_SRV_INTERVAL))
                .await;
        }
    }

    async fn update_hosts(
        &mut self,
        topology_state: &mut TopologyState,
    ) -> Option<TopologyDescriptionDiff> {
        let hosts = match self.lookup_hosts().await {
            Ok(hosts) if hosts.is_empty() => {
                self.no_valid_hosts(None);

                return None;
            }
            Ok(hosts) => hosts,
            Err(err) => {
                self.no_valid_hosts(Some(err));

                return None;
            }
        };

        // TODO: Use TTL values to determine delay duration once `trust-dns-resolver`
        // releases again.
        self.rescan_interval = None;

        topology_state.update_hosts(&hosts.into_iter().collect(), &self.client_options)
    }

    async fn lookup_hosts(&mut self) -> Result<Vec<StreamAddress>> {
        let initial_hostname = self.initial_hostname.clone();
        let resolver = self.get_or_create_srv_resolver().await?;
        let mut new_hosts = Vec::new();

        for host in resolver.get_srv_hosts(&initial_hostname).await? {
            match host {
                Ok(host) => new_hosts.push(host),
                Err(_) => {
                    // TODO RUST-230: Log error with host that was returned.
                }
            }
        }

        Ok(new_hosts)
    }

    async fn get_or_create_srv_resolver(&mut self) -> Result<&mut SrvResolver> {
        if let Some(ref mut resolver) = self.resolver {
            return Ok(resolver);
        }

        let resolver = SrvResolver::new().await?;

        // Since the connection was not `Some` above, this will always insert the new connection and
        // return a reference to it.
        Ok(self.resolver.get_or_insert(resolver))
    }

    fn no_valid_hosts(&mut self, _error: Option<Error>) {
        // TODO RUST-230: Log error/lack of valid results.

        self.rescan_interval = Some(self.heartbeat_freq());
    }

    fn heartbeat_freq(&self) -> Duration {
        self.client_options
            .heartbeat_freq
            .unwrap_or(DEFAULT_HEARTBEAT_FREQUENCY)
    }
}
