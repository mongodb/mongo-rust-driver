use std::time::{Duration, Instant};

use crate::RUNTIME;

/// Interval implementation using async-std.
/// For tokio, we just use tokio::time::Interval.
pub(crate) struct Interval {
    interval: Duration,
    last_time: Instant,
}

#[cfg(feature = "async-std-runtime")]
impl Interval {
    pub(super) fn new(interval: Duration) -> Self {
        Self {
            interval,
            last_time: Instant::now(),
        }
    }

    pub(crate) async fn tick(&mut self) -> Instant {
        match self.interval.checked_sub(self.last_time.elapsed()) {
            Some(duration) => {
                RUNTIME.delay_for(duration).await;
                self.last_time = Instant::now();
            }
            None => self.last_time = Instant::now(),
        }
        self.last_time
    }
}
