mod rtt;
mod sdam;

use std::time::Duration;

pub(crate) fn f64_ms_as_duration(f: f64) -> Duration {
    Duration::from_micros((f * 1000.0) as u64)
}
