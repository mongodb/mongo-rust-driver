use std::{collections::HashSet, time::Duration};

use once_cell::sync::Lazy;
use pretty_assertions::assert_eq;

use super::{LookupHosts, SrvPollingMonitor};
use crate::{
    error::Result,
    options::{ClientOptions, ServerAddress},
    sdam::Topology,
    test::{get_client_options, log_uncaptured},
};

fn localhost_test_build_10gen(port: u16) -> ServerAddress {
    ServerAddress::Tcp {
        host: "localhost.test.build.10gen.cc".into(),
        port: Some(port),
    }
}

static DEFAULT_HOSTS: Lazy<Vec<ServerAddress>> = Lazy::new(|| {
    vec![
        localhost_test_build_10gen(27017),
        localhost_test_build_10gen(27108),
    ]
});

async fn run_test(new_hosts: Result<Vec<ServerAddress>>, expected_hosts: HashSet<ServerAddress>) {
    run_test_srv(None, new_hosts, expected_hosts).await
}

async fn run_test_srv(
    max_hosts: Option<u32>,
    new_hosts: Result<Vec<ServerAddress>>,
    expected_hosts: HashSet<ServerAddress>,
) {
    let actual = run_test_extra(max_hosts, new_hosts).await;
    assert_eq!(expected_hosts, actual);
}

async fn run_test_extra(
    max_hosts: Option<u32>,
    new_hosts: Result<Vec<ServerAddress>>,
) -> HashSet<ServerAddress> {
    let mut options = ClientOptions::new_srv();
    options.hosts.clone_from(&DEFAULT_HOSTS);
    options.test_options_mut().disable_monitoring_threads = true;
    options.srv_max_hosts = max_hosts;
    let mut topology = Topology::new(options.clone()).unwrap();
    topology.watch().wait_until_initialized().await;
    let mut monitor =
        SrvPollingMonitor::new(topology.clone_updater(), topology.watch(), options.clone())
            .unwrap();
    monitor
        .update_hosts(new_hosts.and_then(make_lookup_hosts))
        .await;

    topology.server_addresses()
}

fn make_lookup_hosts(hosts: Vec<ServerAddress>) -> Result<LookupHosts> {
    Ok(LookupHosts {
        hosts,
        min_ttl: Duration::from_secs(60),
    })
}

// If a new DNS record is returned, it should be reflected in the topology.
#[tokio::test]
async fn add_new_dns_record() {
    let hosts = vec![
        localhost_test_build_10gen(27017),
        localhost_test_build_10gen(27018),
        localhost_test_build_10gen(27019),
    ];

    run_test(Ok(hosts.clone()), hosts.into_iter().collect()).await;
}

// If a DNS record is no longer returned, it should be reflected in the topology.
#[tokio::test]
async fn remove_dns_record() {
    let hosts = vec![localhost_test_build_10gen(27017)];

    run_test(Ok(hosts.clone()), hosts.into_iter().collect()).await;
}

// If a single DNS record is replaced, it should be reflected in the topology.
#[tokio::test]
async fn replace_single_dns_record() {
    let hosts = vec![
        localhost_test_build_10gen(27017),
        localhost_test_build_10gen(27019),
    ];

    run_test(Ok(hosts.clone()), hosts.into_iter().collect()).await;
}

// If all DNS records are replaced, it should be reflected in the topology.
#[tokio::test]
async fn replace_all_dns_records() {
    let hosts = vec![localhost_test_build_10gen(27019)];

    run_test(Ok(hosts.clone()), hosts.into_iter().collect()).await;
}

// If a timeout error occurs, the topology should be unchanged.
#[tokio::test]
async fn timeout_error() {
    run_test(
        Err(std::io::ErrorKind::TimedOut.into()),
        DEFAULT_HOSTS.iter().cloned().collect(),
    )
    .await;
}

// If no results are returned, the topology should be unchanged.
#[tokio::test]
async fn no_results() {
    run_test(Ok(Vec::new()), DEFAULT_HOSTS.iter().cloned().collect()).await;
}

// SRV polling is not done for load-balanced clusters (as per spec at
// https://github.com/mongodb/specifications/blob/master/source/polling-srv-records-for-mongos-discovery/tests/README.rst#test-that-srv-polling-is-not-done-for-load-balalanced-clusters).
#[tokio::test]
async fn load_balanced_no_srv_polling() {
    if get_client_options().await.load_balanced != Some(true) {
        log_uncaptured("skipping load_balanced_no_srv_polling due to not load balanced topology");
        return;
    }

    let hosts = vec![localhost_test_build_10gen(27017)];
    let mut options = ClientOptions::new_srv();
    let rescan_interval = options.original_srv_info.as_ref().cloned().unwrap().min_ttl;
    options.hosts.clone_from(&hosts);
    options.load_balanced = Some(true);
    options.test_options_mut().mock_lookup_hosts = Some(make_lookup_hosts(vec![
        localhost_test_build_10gen(27017),
        localhost_test_build_10gen(27018),
    ]));
    let mut topology = Topology::new(options).unwrap();
    topology.watch().wait_until_initialized().await;
    tokio::time::sleep(rescan_interval * 2).await;
    assert_eq!(
        hosts.into_iter().collect::<HashSet<_>>(),
        topology.server_addresses()
    );
}

// SRV polling with srvMaxHosts MongoClient option: All DNS records are selected (srvMaxHosts = 0)
#[tokio::test]
async fn srv_max_hosts_zero() {
    let hosts = vec![
        localhost_test_build_10gen(27017),
        localhost_test_build_10gen(27019),
        localhost_test_build_10gen(27020),
    ];

    run_test_srv(None, Ok(hosts.clone()), hosts.clone().into_iter().collect()).await;
    run_test_srv(Some(0), Ok(hosts.clone()), hosts.into_iter().collect()).await;
}

// SRV polling with srvMaxHosts MongoClient option: All DNS records are selected (srvMaxHosts >=
// records)
#[tokio::test]
async fn srv_max_hosts_gt_actual() {
    let hosts = vec![
        localhost_test_build_10gen(27019),
        localhost_test_build_10gen(27020),
    ];

    run_test_srv(Some(2), Ok(hosts.clone()), hosts.into_iter().collect()).await;
}

// SRV polling with srvMaxHosts MongoClient option: New DNS records are randomly selected
// (srvMaxHosts > 0)
#[tokio::test]
async fn srv_max_hosts_random() {
    let hosts = vec![
        localhost_test_build_10gen(27017),
        localhost_test_build_10gen(27019),
        localhost_test_build_10gen(27020),
    ];

    let actual = run_test_extra(Some(2), Ok(hosts)).await;
    assert_eq!(2, actual.len());
    assert!(actual.contains(&localhost_test_build_10gen(27017)));
}
