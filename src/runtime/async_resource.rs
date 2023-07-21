use std::sync::{Arc, Weak};

use tokio::sync::{Mutex, MutexGuard};

use crate::error::{Error, ErrorKind, Result};

#[async_trait::async_trait]
pub(crate) trait AsyncDrop {
    async fn async_drop(&mut self);
}

#[async_trait::async_trait]
impl<T: AsyncDrop + Send> AsyncDrop for Option<T> {
    async fn async_drop(&mut self) {
        if let Some(mut v) = self.take() {
            v.async_drop().await;
        }
    }
}

pub(crate) struct AsyncResource<T> where T: AsyncDrop + Send + 'static {
    value: Arc<Mutex<Option<T>>>,
}

fn shutdown_err<T>() -> Result<T> {
    Result::Err(Error::new(ErrorKind::Shutdown, None::<Vec<String>>))
}

impl<T: AsyncDrop + Send + 'static> AsyncResource<T> {
    pub(crate) async fn get(&self) -> Result<Guard<'_, T>> {
        let inner = self.value.lock().await;
        if inner.is_none() {
            return shutdown_err();
        }
        Ok(Guard { inner })
    }

    async fn shutdown(self) {
        self.value.lock().await.async_drop().await;
    }
}

impl<T: AsyncDrop + Send + 'static> Drop for AsyncResource<T> {
    fn drop(&mut self) {
        let mut v = crate::runtime::block_on(async { 
            self.value.lock().await.take()
        });
        crate::runtime::spawn(async move { v.async_drop().await });
    }
}

pub(crate) struct Guard<'a, T> {
    inner: MutexGuard<'a, Option<T>>,
}

impl<'a, T> std::ops::Deref for Guard<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().unwrap()
    }
}

impl<'a, T> std::ops::DerefMut for Guard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().unwrap()
    }
}

impl<'a, T> Drop for Guard<'a, T> {
    fn drop(&mut self) {
        
    }
}

#[derive(Debug)]
pub(crate) struct Registry {
    // TODO: this is a memory leak; needs a garbage collection task or something?
    children: Vec<Weak<Mutex<dyn AsyncDrop + Send + Sync>>>,
}

impl Registry {
    pub(crate) fn new() -> Self {
        Self { children: vec![] }
    }

    pub(crate) fn add<T: AsyncDrop + Send + Sync + 'static>(&mut self, value: T) -> AsyncResource<T> {
        let value = Arc::new(Mutex::new(Some(value)));
        self.children.push(Arc::downgrade(&value) as Weak<Mutex<_>>);
        AsyncResource { value }
    }

    pub(crate) async fn shutdown(&mut self) {
        // TODO: This could be parallelized if it turns out to be a bottleneck.
        for child in self.children.drain(..) {
            if let Some(value) = child.upgrade() {
                value.lock().await.async_drop().await;
            }
        }
    }
}