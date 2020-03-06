use std::{
    time::Duration,
    convert::TryInto,
};

use futures_intrusive::sync::{SemaphoreReleaser, Semaphore};

use crate::{
    error::{ErrorKind, Result},
    options::StreamAddress,
    RUNTIME,
};

/// The wait queue ensures that threads acquiring connections proceed in a first-come, first-serve
/// order.
#[derive(Debug)]
pub(crate) struct WaitQueue {
    /// The maximum number of threads that can hold a permit to the semaphore.
    /// This will be the `max_pool_size` for a given connection pool.
    max_handles: usize,

    /// A fair counting semaphore whose count corresponds to the number of connections available.
    semaphore: Semaphore,

    /// The address that the connection pool's connections will connect to. This is needed to
    /// return a WaitQueueTimeoutError when the timeout has elapsed.
    address: StreamAddress,

    /// The timeout signifying how long a thread should wait in the queue before returning an
    /// error. This will be the `wait_queue_timeout` for a given connection pool.
    timeout: Option<Duration>,
}

impl WaitQueue {
    /// Creat a new `WaitQueue`.
    pub(super) fn new(address: StreamAddress, max_handles: u32, timeout: Option<Duration>) -> Self {
        let max_handles = max_handles.try_into().unwrap_or(usize::max_value());

        Self {
            semaphore: Semaphore::new(true, max_handles),
            address,
            timeout,
            max_handles,
        }
    }

    /// Enter the wait queue and block until either reaching the front of the queue or
    /// exceeding the timeout.
    pub(super) async fn wait_until_at_front(&self) -> Result<WaitQueueHandle<'_>> {
        let future = self.semaphore.acquire(1);

        let releaser = if let Some(timeout) = self.timeout {
            RUNTIME.await_with_timeout(Box::pin(future), timeout).await.map_err(|_| {
                ErrorKind::WaitQueueTimeoutError {
                    address: self.address.clone(),
                }
            })?
        } else {
            future.await
        };

        Ok(WaitQueueHandle {
            semaphore_releaser: releaser
        })
    }

    /// Signal that the front of the queue (if there is one) is ready to wake up.
    pub(super) fn wake_front(&self) {
        if self.semaphore.permits() >= self.max_handles {
            panic!("greater than {} connections checked back into pool with address {}",
                   self.max_handles,
                   self.address.clone()
            );
        }
        self.semaphore.release(1);
    }
}

/// A handle to a `WaitQueue` that will wake up the front of the queue when dropped.
/// To disable this behavior, call `WaitQueueHandle::disarm`.
pub(super) struct WaitQueueHandle<'a> {
    semaphore_releaser: SemaphoreReleaser<'a>
}

impl<'a> WaitQueueHandle<'a> {
    pub(super) fn disarm(&mut self) {
        self.semaphore_releaser.disarm();
    }
}
