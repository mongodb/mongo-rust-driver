use crate::test::{spec::unified_runner::run_unified_tests, LOCK};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))] // multi_thread required for FailPoint
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_unified() {
    let _guard = LOCK.run_exclusively().await;
    run_unified_tests(&["run-command", "unified"]).skip_files(&[
        "runCursorCommand.json",
    ]).skip_tests(&[
        //TODO: fix withTransaction for new test runner
        "attaches transaction fields to given command"
    ]).await;
} 