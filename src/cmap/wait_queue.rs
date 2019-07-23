use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex, MutexGuard},
    time::Duration,
};

use derivative::Derivative;

use crate::{
    error::{ErrorKind, Result},
    event::cmap::{
        CmapEventHandler, ConnectionCheckoutFailedEvent, ConnectionCheckoutFailedReason,
    },
};

// The wait queue ensures that threads acquiring connections proceed in a first-come, first-serve
// order. We wrap the internal state in an both an `Arc` and a `Mutex`; the `Arc` allows us to share
// the state across threads, and the mutex allows us to predicate access to the wait queue on a
// `Condvar`.
#[derive(Debug)]
pub(crate) struct WaitQueue {
    inner: Arc<Mutex<WaitQueueInner>>,
}

// The internal state of the wait queue.
#[derive(Derivative)]
#[derivative(Debug)]
struct WaitQueueInner {
    // The elements in the queue are conditional variables. When a thread enters the wait queue,
    // they block on a newly-created conditional variable until either they are at the front of the
    // queue or an optional timeout is reached.
    queue: VecDeque<Arc<Condvar>>,

    // The timeout signifying how long a thread should wait in the queue before returning an error.
    // This will be the `wait_queue_timeout` for a given connection pool.
    timeout: Option<Duration>,

    // The address of the server that the connection pool's connections will connect to. This is
    // needed to emit a ConnectionCheckoutFailedEvent when the timeout has elapsed.
    address: String,

    // The event handler specified by the user to process CMAP events.
    #[derivative(Debug = "ignore")]
    event_handler: Option<Arc<dyn CmapEventHandler>>,
}

// A thread will obtain a `WaitQueueHandle` when it reaches the front of the wait queue. This gives
// it access to the queue itself, which it can use to pop itself off the queue when the checkout
// operation finishes, as well as a conditional variable that it can use to block on until a
// connection is ready.
pub(crate) struct WaitQueueHandle<'a> {
    // Although a `WaitQueueHandle` will always have an associated mutex guard, we need to
    // temporarily pass ownership of the guard to `Condvar::wait` (or `Condvar::wait_timeout`),
    // which will return the mutex guard when it's done blocking. Since we are unable to take
    // direct ownership of a field in a method that does not take ownership, we're forced to make
    // the `guard` field an `Option` and temporarily remove the guard from the `Option` while
    // waiting.
    guard: Option<MutexGuard<'a, WaitQueueInner>>,

    // The conditional variable used to facilitate waiting for a connection to be ready.
    condvar: Arc<Condvar>,
}

impl<'a> WaitQueueHandle<'a> {
    // Creates a new `WaitQueueHandle`.
    fn new(guard: MutexGuard<'a, WaitQueueInner>, condvar: Arc<Condvar>) -> Self {
        Self {
            guard: Some(guard),
            condvar,
        }
    }

    // Blocks on the internal conditional variable until either the conditional variable is notified
    // (which indicates that a connection is ready) or a timeout occurs.
    pub(crate) fn wait(&mut self, timeout: Option<Duration>) -> Result<()> {
        // Temporarily remove the mutex guard from the field so that we can pass ownership of it to
        // the conditional variable.
        let guard = self.guard.take().unwrap();

        if let Some(timeout) = timeout {
            let (guard, result) = self.condvar.wait_timeout(guard, timeout).unwrap();

            if result.timed_out() {
                if let Some(ref event_handler) = guard.event_handler {
                    let event = ConnectionCheckoutFailedEvent {
                        address: guard.address.clone(),
                        reason: ConnectionCheckoutFailedReason::Timeout,
                    };

                    event_handler.handle_connection_checkout_failed_event(event);
                }

                bail!(ErrorKind::WaitQueueTimeoutError(guard.address.clone()));
            }

            self.guard = Some(guard);
        } else {
            self.guard = Some(self.condvar.wait(guard).unwrap());
        }

        Ok(())
    }
}

impl<'a> Drop for WaitQueueHandle<'a> {
    // When the `WaitQueueHandle` goes out of scope, we pop the current thread from the front of the
    // wait queue and notify the wait queue that it can wake up the next thread.
    fn drop(&mut self) {
        if let Some(ref mut guard) = self.guard {
            guard.queue.pop_front();
            guard.notify_ready();
        }
    }
}

impl WaitQueue {
    // Creates a new `WaitQueue`.
    pub(crate) fn new(
        address: &str,
        timeout: Option<Duration>,
        event_handler: Option<Arc<dyn CmapEventHandler>>,
    ) -> Self {
        let inner = WaitQueueInner {
            queue: Default::default(),
            address: address.to_string(),
            event_handler,
            timeout,
        };

        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    // Enters the wait queue and blocks until it either reaches the front of the queue or the
    // timeout has elapsed.
    pub(crate) fn wait_until_at_front(&self) -> Result<WaitQueueHandle> {
        let mut guard = self.inner.lock().unwrap();

        let condvar = Arc::new(Condvar::new());
        guard.queue.push_back(condvar.clone());

        // If we're already at the front of the wait queue, we don't need to wait. We don't pop
        // ourself off of the wait queue yet though, since another thread may enter while we're
        // waiting for a connection to become available.
        if guard.queue.len() == 1 {
            return Ok(WaitQueueHandle::new(guard, condvar));
        }

        let guard = if let Some(timeout) = guard.timeout {
            let (guard, result) = condvar.wait_timeout(guard, timeout).unwrap();

            if result.timed_out() {
                if let Some(ref event_handler) = guard.event_handler {
                    let event = ConnectionCheckoutFailedEvent {
                        address: guard.address.clone(),
                        reason: ConnectionCheckoutFailedReason::Timeout,
                    };

                    event_handler.handle_connection_checkout_failed_event(event);
                }

                bail!(ErrorKind::WaitQueueTimeoutError(guard.address.clone()));
            }

            guard
        } else {
            condvar.wait(guard).unwrap()
        };

        Ok(WaitQueueHandle::new(guard, condvar))
    }

    // Notifies the wait queue that the next thread in the queue can be woken up.
    pub(crate) fn notify_ready(&self) {
        self.inner.lock().unwrap().notify_ready();
    }
}

impl WaitQueueInner {
    // Notifies the wait queue that the next thread in the queue can be woken up. This is explicitly
    // defined on `WaitQueueInner` so that `WaitQueueHandle::drop` can call it.
    pub(crate) fn notify_ready(&self) {
        if let Some(condvar) = self.queue.front() {
            condvar.notify_one();
        }
    }
}
