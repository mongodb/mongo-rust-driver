//! Contains the sync API. This is only available when the `sync` feature is enabled.

mod change_stream;
mod client;
mod coll;
mod cursor;
mod db;
pub mod gridfs;

#[cfg(test)]
mod test;

pub use change_stream::{ChangeStream, SessionChangeStream};
pub use client::{session::ClientSession, Client};
pub use coll::Collection;
pub use cursor::{Cursor, SessionCursor, SessionCursorIter};
pub use db::Database;

#[cfg(feature = "tokio-sync")]
pub(crate) static TOKIO_RUNTIME: once_cell::sync::Lazy<tokio::runtime::Runtime> =
    once_cell::sync::Lazy::new(|| match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(err) => panic!(
            "Error occurred when starting the underlying async runtime: {}",
            err
        ),
    });
