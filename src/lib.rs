//! This crate is a pure Rust MongoDB driver. It follows the
//! [MongoDB driver API and feature specifications](https://github.com/mongodb/specifications).
//!
//! To connect to a MongoDB database, pass a MongoDB connection string to `Client::connect`:
//!
//! ```rust
//! # use mongodb::{Client, error::Result};
//! #
//! # fn make_client() -> Result<Client> {
//! let client = Client::with_uri_str("mongodb://localhost:27017/")?;
//! # Ok(client)
//! # }
//! ```
//!
//! Operations can be performed by obtaining a `Database` or `Collection` from the `Client`:
//!
//! ```rust
//! # use bson::{bson, doc};
//! # use mongodb::{Client, error::Result};
//! #
//! # fn do_stuff() -> Result<()> {
//! # let client = Client::with_uri_str("mongodb://localhost:27017")?;
//!
//! let db = client.database("some_db");
//! for coll_name in db.list_collection_names(None)? {
//!     println!("collection: {}", coll_name);
//! }
//!
//! let coll = db.collection("some-coll");
//! let result = coll.insert_one(doc! { "x": 1 }, None)?;
//! println!("{:#?}", result);
//!
//! # Ok(())
//! # }
//! ```

#![recursion_limit = "128"]
#![cfg_attr(
    feature = "cargo-clippy",
    allow(
        clippy::unreadable_literal,
        clippy::cognitive_complexity,
        clippy::float_cmp
    )
)]

#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate bson;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate typed_builder;

#[macro_use]
pub mod read_preference;

mod bson_util;
mod client;
mod coll;
pub mod collation;
mod command_responses;
pub mod concern;
mod cursor;
mod db;
pub mod error;
pub mod options;
mod pool;
pub mod results;
#[cfg(test)]
mod test;
mod topology;
mod wire;

pub use crate::{client::Client, coll::Collection, cursor::Cursor, db::Database};
