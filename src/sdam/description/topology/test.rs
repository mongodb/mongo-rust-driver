mod event;
mod rtt;
mod sdam;

use std::time::Duration;

pub use event::TestSdamEvent;

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub(crate) fn f64_ms_as_duration(f: f64) -> Duration {
    Duration::from_micros((f * 1000.0) as u64)
}
