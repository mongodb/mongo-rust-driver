use std::collections::HashSet;

use pretty_assertions::assert_eq;

use super::{LookupHosts, SrvPollingMonitor};
use crate::{
    error::{ErrorKind, Result},
    options::StreamAddress,
    sdam::Topology,
};

fn localhost_test_build_10gen(port: u16) -> StreamAddress {
    StreamAddress {
        hostname: "localhost.test.build.10gen.cc".into(),
        port: Some(port),
    }
}

lazy_static::lazy_static! {
    static ref DEFAULT_HOSTS: Vec<StreamAddress> = vec![
        localhost_test_build_10gen(27017),
        localhost_test_build_10gen(27108),
    ];
}

async fn run_test(new_hosts: Result<Vec<StreamAddress>>, expected_hosts: HashSet<StreamAddress>) {
    let topology = Topology::new_from_hosts(DEFAULT_HOSTS.iter());
    let state = topology.clone_state().await;
    let mut monitor = SrvPollingMonitor::new(topology.downgrade()).unwrap();

    monitor
        .update_hosts(
            new_hosts.map(|hosts| LookupHosts {
                hosts,
                min_ttl: None,
            }),
            topology.clone(),
            state,
        )
        .await;

    assert_eq!(expected_hosts, topology.servers().await);
}

// If a new DNS record is returned, it should be reflected in the topology.
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn add_new_dns_record() {
    let hosts = vec![
        localhost_test_build_10gen(27017),
        localhost_test_build_10gen(27018),
        localhost_test_build_10gen(27019),
    ];

    run_test(Ok(hosts.clone()), hosts.into_iter().collect()).await;
}

// If a DNS record is no longer returned, it should be reflected in the topology.
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn remove_dns_record() {
    let hosts = vec![localhost_test_build_10gen(27017)];

    run_test(Ok(hosts.clone()), hosts.into_iter().collect()).await;
}

// If a single DNS record is replaced, it should be reflected in the topology.
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn replace_single_dns_record() {
    let hosts = vec![
        localhost_test_build_10gen(27017),
        localhost_test_build_10gen(27019),
    ];

    run_test(Ok(hosts.clone()), hosts.into_iter().collect()).await;
}

// If all DNS records are replaced, it should be reflected in the topology.
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn replace_all_dns_records() {
    let hosts = vec![localhost_test_build_10gen(27019)];

    run_test(Ok(hosts.clone()), hosts.into_iter().collect()).await;
}

// If a timeout error occurs, the topology should be unchanged.
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn timeout_error() {
    run_test(
        Err(ErrorKind::Io(std::io::ErrorKind::TimedOut.into()).into()),
        DEFAULT_HOSTS.iter().cloned().collect(),
    )
    .await;
}

// If no results are returned, the topology should be unchanged.
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn no_results() {
    run_test(Ok(Vec::new()), DEFAULT_HOSTS.iter().cloned().collect()).await;
}
