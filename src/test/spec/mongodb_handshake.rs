use crate::test::topology_is_sharded;

use super::unified_runner::run_unified_tests;

#[tokio::test]
async fn run_unified() {
    let mut runner = run_unified_tests(&["mongodb-handshake", "unified"]);
    if topology_is_sharded().await {
        // This test is flaky on sharded deployments.
        runner = runner.skip_tests(
            &[
                "metadata append does not create new connections or close existing ones and no \
                 hello command is sent",
            ],
        );
    }
    runner.await;
}
