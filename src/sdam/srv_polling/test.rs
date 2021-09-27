use std::{collections::HashSet, time::Duration};

use pretty_assertions::assert_eq;

use super::{LookupHosts, SrvPollingMonitor};
use crate::{
    error::Result,
    options::{ClientOptions, ServerAddress},
    sdam::Topology,
    RUNTIME,
};

fn localhost_test_build_10gen(port: u16) -> ServerAddress {
    ServerAddress::Tcp {
        host: "localhost.test.build.10gen.cc".into(),
        port: Some(port),
    }
}

lazy_static::lazy_static! {
    static ref DEFAULT_HOSTS: Vec<ServerAddress> = vec![
        localhost_test_build_10gen(27017),
        localhost_test_build_10gen(27108),
    ];
}

async fn run_test(new_hosts: Result<Vec<ServerAddress>>, expected_hosts: HashSet<ServerAddress>) {
    let mut options = ClientOptions::new_srv();
    options.hosts = DEFAULT_HOSTS.clone();
    options.test_options_mut().disable_monitoring_threads = true;
    let topology = Topology::new(options).unwrap();
    let mut monitor = SrvPollingMonitor::new(topology.downgrade()).unwrap();
    monitor
        .update_hosts(new_hosts.and_then(make_lookup_hosts), topology.clone())
        .await;

    assert_eq!(expected_hosts, topology.servers().await);
}

fn make_lookup_hosts(hosts: Vec<ServerAddress>) -> Result<LookupHosts> {
    Ok(LookupHosts {
        hosts: hosts.into_iter().map(Result::Ok).collect(),
        min_ttl: Duration::from_secs(60),
    })
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
        Err(std::io::ErrorKind::TimedOut.into()),
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

// SRV polling is not done for load-balanced clusters (as per spec at
// https://github.com/mongodb/specifications/blob/master/source/polling-srv-records-for-mongos-discovery/tests/README.rst#test-that-srv-polling-is-not-done-for-load-balalanced-clusters).
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn load_balanced_no_srv_polling() {
    let hosts = vec![localhost_test_build_10gen(27017)];
    let mut options = ClientOptions::new_srv();
    let rescan_interval = options.original_srv_info.as_ref().cloned().unwrap().min_ttl;
    options.hosts = hosts.clone();
    options.load_balanced = Some(true);
    options.test_options_mut().mock_lookup_hosts = Some(make_lookup_hosts(vec![
        localhost_test_build_10gen(27017),
        localhost_test_build_10gen(27018),
    ]));
    let topology = Topology::new(options).unwrap();
    RUNTIME.delay_for(rescan_interval * 2).await;
    assert_eq!(
        hosts.into_iter().collect::<HashSet<_>>(),
        topology.servers().await
    );
}
