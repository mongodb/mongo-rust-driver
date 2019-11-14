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

#![allow(unused_variables)]
#![cfg_attr(
    feature = "cargo-clippy",
    allow(
        clippy::unreadable_literal,
        clippy::cognitive_complexity,
        clippy::float_cmp
    )
)]

#[macro_use]
extern crate derivative;
#[macro_use]
extern crate typed_builder;

#[macro_use]
pub mod options;

mod bson_util;
mod client;
#[allow(dead_code)]
mod cmap;
mod coll;
pub mod concern;
mod cursor;
mod db;
pub mod error;
pub mod event;
mod is_master;
mod operation;
pub mod read_preference;
pub mod results;
#[allow(dead_code)]
mod sdam;
#[cfg(test)]
mod test;

#[cfg(test)]
#[macro_use]
extern crate derive_more;

pub use crate::{
    client::Client,
    coll::{Collection, Namespace},
    cursor::Cursor,
    db::Database,
};
