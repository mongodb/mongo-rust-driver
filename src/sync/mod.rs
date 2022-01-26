//! Contains the sync API. This is only available when the `sync` feature is enabled.

mod change_stream;
mod client;
mod coll;
mod cursor;
mod db;

#[cfg(test)]
mod test;

pub use change_stream::ChangeStream;
pub use client::{session::ClientSession, Client};
pub use coll::Collection;
pub use cursor::{Cursor, SessionCursor, SessionCursorIter};
pub use db::Database;
