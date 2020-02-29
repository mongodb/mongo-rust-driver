use std::time::Duration;
use std::convert::TryInto;

use futures_intrusive::sync::Semaphore;

use crate::{
    error::{ErrorKind, Result},
    options::StreamAddress,
    runtime::Timeoutable
};

/// The wait queue ensures that threads acquiring connections proceed in a first-come, first-serve
/// order.
#[derive(Debug)]
pub(crate) struct WaitQueue {
    semaphore: Semaphore,
    address: StreamAddress,
    timeout: Option<Duration>,
}

impl WaitQueue {
    /// Creat a new `WaitQueue`.
    pub(super) fn new(address: StreamAddress, max_handles: u32, timeout: Option<Duration>) -> Self {
        let max_handles = max_handles.try_into().unwrap_or(usize::max_value());

        Self {
            address,
            semaphore: Semaphore::new(true, max_handles),
            timeout
        }
    }

    /// Enter the wait queue and block until either reaching the front of the queue or
    /// exceeding the timeout.
    pub(super) async fn wait_until_at_front(&self) -> Result<()> {
        let future = self.semaphore.acquire(1);

        let mut releaser = if let Some(timeout) = self.timeout {
            Box::pin(future).with_timeout(timeout, || ErrorKind::WaitQueueTimeoutError {
                address: self.address.clone(),
            }.into()).await?
        } else {
            future.await
        };
        releaser.disarm(); // releasing is handled during connection drop
        
        Ok(())
    }

    /// Signal that the front of the queue (if there is one) is ready to wake up.
    pub(super) fn wake_front(&self) {
        self.semaphore.release(1);
    }
}
