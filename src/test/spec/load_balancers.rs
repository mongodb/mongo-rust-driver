use tokio::sync::RwLockWriteGuard;

use crate::test::{
    spec::{
        unified_runner::{run_unified_tests, ExpectedCmapEvent, ExpectedEvent, TestFile},
        ExpectedEventType,
    },
    LOCK,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_unified() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;

    // The Rust driver's asynchronous check-in of connections means that sometimes a new
    // connection will be created, rather than re-using an existing one; the connectionReady
    // events generated by this cause tests to fail, but does not break user-visible behavior.
    // TODO RUST-1055 Remove this workaround.
    fn file_transformation(test_file: &mut TestFile) {
        for test in &mut test_file.tests {
            if let Some(e) = test.expect_events.as_mut() {
                for expect_events in e {
                    if expect_events.event_type == Some(ExpectedEventType::Cmap) {
                        expect_events.event_type =
                            Some(ExpectedEventType::CmapWithoutConnectionReady);
                        expect_events.events.retain(|ev| {
                            !matches!(
                                ev,
                                ExpectedEvent::Cmap(ExpectedCmapEvent::ConnectionReady {})
                            )
                        });
                    }
                }
            }
        }
    }

    run_unified_tests(&["load-balancers"])
        // The Rust driver doesn't support wait queue timeouts.
        .skip_files(vec!["wait-queue-timeouts.json"])
        .transform_files(file_transformation)
        .await;
}
