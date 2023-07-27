#[cfg(test)]
use crate::id_set::{self, IdSet};
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex as SyncMutex;

/// An `Arc` that records the backtraces of construction of live clones.  When not compiled
/// with `cfg(test)`, a zero-cost `Arc` wrapper.
#[derive(Debug)]
pub(crate) struct TrackingArc<T> {
    inner: Arc<Inner<T>>,
    #[cfg(test)]
    clone_id: Option<id_set::Id>,
}

#[derive(Debug)]
struct Inner<T> {
    value: T,
    #[cfg(test)]
    clones: SyncMutex<IdSet<backtrace::Backtrace>>,
}

impl<T> TrackingArc<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            inner: Arc::new(Inner {
                value,
                #[cfg(test)]
                clones: SyncMutex::new(IdSet::new()),
            }),
            #[cfg(test)]
            clone_id: None,
        }
    }

    #[allow(unused)]
    pub(crate) fn try_unwrap(tracked: Self) -> Result<T, Self> {
        let inner = tracked.inner.clone();
        #[cfg(test)]
        let clone_id = {
            let mut tracked = tracked;
            tracked.clone_id.take()
        };
        match Arc::try_unwrap(inner) {
            Ok(inner) => Ok(inner.value),
            Err(inner) => Err(Self {
                inner,
                #[cfg(test)]
                clone_id,
            }),
        }
    }

    pub(crate) fn downgrade(tracked: &Self) -> Weak<T> {
        Weak {
            inner: Arc::downgrade(&tracked.inner),
        }
    }

    pub(crate) fn ptr_eq(this: &Self, other: &Self) -> bool {
        Arc::ptr_eq(&this.inner, &other.inner)
    }

    #[cfg(test)]
    pub(crate) fn print_live(tracked: &Self) {
        let current: Vec<_> = tracked
            .inner
            .clones
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect();
        for mut bt in current {
            bt.resolve();
            println!("{:?}", bt);
        }
    }
}

impl<T> Clone for TrackingArc<T> {
    fn clone(&self) -> Self {
        #[cfg(test)]
        let clone_id = {
            let bt = backtrace::Backtrace::new_unresolved();
            Some(self.inner.clones.lock().unwrap().insert(bt))
        };
        Self {
            inner: self.inner.clone(),
            #[cfg(test)]
            clone_id,
        }
    }
}

impl<T> Drop for TrackingArc<T> {
    fn drop(&mut self) {
        #[cfg(test)]
        if let Some(id) = &self.clone_id {
            self.inner.clones.lock().unwrap().remove(&id);
        }
    }
}

impl<T> std::ops::Deref for TrackingArc<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner.value
    }
}

#[derive(Debug)]
pub(crate) struct Weak<T> {
    inner: std::sync::Weak<Inner<T>>,
}

impl<T> Clone for Weak<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Weak<T> {
    pub(crate) fn upgrade(&self) -> Option<TrackingArc<T>> {
        self.inner.upgrade().map(|inner| {
            #[cfg(test)]
            let clone_id = {
                let bt = backtrace::Backtrace::new_unresolved();
                Some(inner.clones.lock().unwrap().insert(bt))
            };
            TrackingArc {
                inner,
                #[cfg(test)]
                clone_id,
            }
        })
    }
}
