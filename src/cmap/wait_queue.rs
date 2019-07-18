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

#[derive(Debug)]
pub(crate) struct WaitQueue {
    inner: Arc<Mutex<WaitQueueInner>>,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct WaitQueueInner {
    queue: VecDeque<Arc<Condvar>>,
    timeout: Option<Duration>,
    address: String,

    #[derivative(Debug = "ignore")]
    event_handler: Option<Arc<CmapEventHandler>>,
}

pub(crate) struct WaitQueueHandle<'a> {
    guard: Option<MutexGuard<'a, WaitQueueInner>>,
    condvar: Arc<Condvar>,
}

impl<'a> WaitQueueHandle<'a> {
    fn new(guard: MutexGuard<'a, WaitQueueInner>, condvar: Arc<Condvar>) -> Self {
        Self {
            guard: Some(guard),
            condvar,
        }
    }

    pub(crate) fn wait(&mut self, timeout: Option<Duration>) -> Result<()> {
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
    fn drop(&mut self) {
        if let Some(ref mut guard) = self.guard {
            guard.queue.pop_front();
            guard.notify_ready();
        }
    }
}

impl WaitQueue {
    pub(crate) fn new(
        address: &str,
        timeout: Option<Duration>,
        event_handler: Option<Arc<CmapEventHandler>>,
    ) -> Self {
        let inner = WaitQueueInner {
            queue: Default::default(),
            address: address.into(),
            event_handler,
            timeout,
        };

        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub(crate) fn wait_until_at_front(&self) -> Result<WaitQueueHandle> {
        let mut guard = self.inner.lock().unwrap();

        let condvar = Arc::new(Condvar::new());
        guard.queue.push_back(condvar.clone());

        // If we're already at the front of the wait queue, we don't need to wait.
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

    pub(crate) fn notify_ready(&self) {
        self.inner.lock().unwrap().notify_ready();
    }
}

impl WaitQueueInner {
    pub(crate) fn notify_ready(&self) {
        if let Some(condvar) = self.queue.front() {
            condvar.notify_one();
        }
    }
}
