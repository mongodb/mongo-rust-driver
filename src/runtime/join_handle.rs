use std::ops::DerefMut;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::error::{ErrorKind, Result};

#[derive(Debug)]
pub(crate) enum AsyncJoinHandle<T> {
    /// Wrapper around `tokio::net:TcpStream`.
    #[cfg(feature = "tokio-runtime")]
    Tokio(tokio::task::JoinHandle<T>),
}

impl<T> Future for AsyncJoinHandle<T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.deref_mut() {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio(ref mut handle) => {
                Pin::new(handle).poll(cx)
                    .map(|result| {
                        result.map_err(|e| ErrorKind::InternalError { message: format!("{}", e) }.into())
                    })
            }
        }
    }
}
