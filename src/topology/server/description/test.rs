use super::ServerDescription;

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
    S(String),
}

fn run_test(test_file: TestFile) {
    let avg_rtt_ms = match test_file.avg_rtt_ms {
        AverageRtt::F(f) => Some(f),
        AverageRtt::S(ref s) if s == "NULL" => None,
        AverageRtt::S(ref s) => panic!("invalid average round trip time: {}", s),
    };

    // The address is not used, so it doesn't matter.
    let mut server_desc = ServerDescription::new("localhost:27017", None);
    server_desc.round_trip_time = Some(test_file.new_rtt_ms);
    server_desc.update_round_trip_time(avg_rtt_ms);

    assert_eq!(server_desc.round_trip_time, Some(test_file.new_avg_rtt));
}

#[test]
fn server_selection_rtt() {
    crate::test::run(&["server-selection", "rtt"], run_test);
}
