#[cfg(test)]
mod test;

use std::time::Duration;

use super::{
    monitor::DEFAULT_HEARTBEAT_FREQUENCY,
    state::{Topology, WeakTopology},
};
use crate::{
    error::{Error, Result},
    options::{ClientOptions, StreamAddress},
    srv::SrvResolver,
    RUNTIME,
};

const DEFAULT_RESCAN_SRV_INTERVAL: Duration = Duration::from_secs(60);

pub(crate) struct SrvPollingMonitor {
    initial_hostname: String,
    resolver: Option<SrvResolver>,
    topology: WeakTopology,
    rescan_interval: Option<Duration>,
    client_options: ClientOptions,
}

struct LookupHosts {
    hosts: Vec<StreamAddress>,
    min_ttl: Option<Duration>,
}

impl SrvPollingMonitor {
    pub(crate) fn new(topology: WeakTopology) -> Option<Self> {
        let client_options = topology.client_options().clone();

        let initial_hostname = match client_options.original_srv_hostname() {
            Some(hostname) => hostname.clone(),
            None => return None,
        };

        Some(Self {
            initial_hostname,
            resolver: None,
            topology,
            rescan_interval: None,
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

    async fn execute(&mut self) {
        while self.topology.is_alive() {
            let topology = match self.topology.upgrade() {
                Some(topology) => topology,
                None => break,
            };

            if topology.is_sharded().await || topology.is_unknown().await {
                let hosts = self.lookup_hosts().await;
                self.update_hosts(hosts, topology.clone()).await;
            }

            std::mem::drop(topology);

            RUNTIME
                .delay_for(self.rescan_interval.unwrap_or(DEFAULT_RESCAN_SRV_INTERVAL))
                .await;
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

        topology
            .update_hosts(lookup.hosts.into_iter().collect(), &self.client_options)
            .await;
    }

    async fn lookup_hosts(&mut self) -> Result<LookupHosts> {
        let initial_hostname = self.initial_hostname.clone();
        let resolver = self.get_or_create_srv_resolver().await?;
        let mut new_hosts = Vec::new();

        for host in resolver.get_srv_hosts(&initial_hostname).await? {
            #[allow(clippy::single_match)]
            match host {
                Ok(host) => new_hosts.push(host),
                Err(_) => {
                    // TODO RUST-230: Log error with host that was returned.
                }
            }
        }

        Ok(LookupHosts {
            hosts: new_hosts,
            min_ttl: resolver
                .min_ttl()
                .map(|ttl| Duration::from_secs(ttl as u64)),
        })
    }

    async fn get_or_create_srv_resolver(&mut self) -> Result<&mut SrvResolver> {
        if let Some(ref mut resolver) = self.resolver {
            return Ok(resolver);
        }

        let resolver = SrvResolver::new(self.client_options.resolver_config.clone()).await?;

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
