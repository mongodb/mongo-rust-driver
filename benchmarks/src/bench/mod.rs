pub mod find_one;
pub mod insert_one;
pub mod run_command;

use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use crate::error::Result;

pub trait Benchmark: Sized {
    type Context;

    // execute once before benchmarking
    fn setup(path: Option<PathBuf>, uri: Option<&str>) -> Result<Self>;

    // execute at the beginning of every iteration
    fn before_task(&self) -> Result<Self::Context>;

    fn do_task(&self, context: Self::Context) -> Result<()>;

    // execute at the end of every iteration
    fn after_task(&self) -> Result<()> {
        Ok(())
    }

    // execute once after benchmarking
    fn teardown(&self) -> Result<()>;
}

pub fn run_benchmark(test: impl Benchmark) -> Result<Vec<Duration>> {
    let mut test_durations = Vec::new();

    for _x in 0..100 {
        let timer = Instant::now();
        let context = test.before_task()?;
        test.do_task(context)?;
        test.after_task()?;
        test_durations.push(timer.elapsed());
    }
    test.teardown()?;

    test_durations.sort();
    Ok(test_durations)
}
