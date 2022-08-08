use std::collections::HashMap;

use serde::Deserialize;

use crate::{
    sdam::{
        description::{
            server::ServerDescription,
            topology::{test::f64_ms_as_duration, TopologyDescription, TopologyType},
        },
        monitor::RttInfo,
    },
    test::run_spec_test,
};

#[derive(Debug, Deserialize)]
pub struct TestFile {
    pub avg_rtt_ms: AverageRtt,
    pub new_rtt_ms: f64,
    pub new_avg_rtt: f64,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum AverageRtt {
    F(f64),
    I(i32),
    S(String),
}

async fn run_test(test_file: TestFile) {
    let avg_rtt_ms = match test_file.avg_rtt_ms {
        AverageRtt::F(f) => Some(f),
        AverageRtt::S(ref s) if s == "NULL" => None,
        AverageRtt::I(i) => Some(i as f64),
        AverageRtt::S(ref s) => panic!("invalid average round trip time: {}", s),
    };

    let rtt_info = RttInfo {
        average: avg_rtt_ms.map(f64_ms_as_duration),
    };
    let new_rtt = rtt_info.with_updated_average_rtt(f64_ms_as_duration(test_file.new_rtt_ms));
    assert_eq!(
        new_rtt.average.unwrap(),
        f64_ms_as_duration(test_file.new_avg_rtt)
    );
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn server_selection_rtt() {
    run_spec_test(&["server-selection", "rtt"], run_test).await;
}
