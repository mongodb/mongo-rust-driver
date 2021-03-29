//! Contains the sync API. This is only available when the `sync` feature is enabled.

mod client;
mod coll;
mod cursor;
mod db;

#[cfg(test)]
mod test;

pub use client::Client;
pub use coll::Collection;
pub use cursor::{Cursor, SessionCursor};
pub use db::Database;
